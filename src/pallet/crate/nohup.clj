(ns pallet.crate.nohup
  "Provides supervision via nohup.  Note that this is very limited, and not
  really recommended for production use."
  (:require
   [clojure.tools.logging :refer [warnf]]
   [pallet.action :refer [with-action-options]]
   [pallet.actions
    :refer [directory exec-checked-script plan-when plan-when-not remote-file]
    :as actions]
   [pallet.crate :refer [target-flag?]]
   [pallet.crate.service
    :refer [service-supervisor service-supervisor-available?
            service-supervisor-config]]
   [pallet.script.lib :refer [state-root file flag?]]
   [pallet.stevedore :refer [fragment script]]
   [pallet.utils :refer [apply-map]]))

(defn nohup-path []
  (fragment (file (state-root) "pallet" "nohup-service")))

(defn service-script-file
  ([service-name filename]
     (fragment (file (nohup-path) ~service-name ~filename)))
  ([service-name]
     (fragment (file (nohup-path) ~service-name))))

(defn service-script-path [service-name]
  (service-script-file service-name "run"))

(defn service-script-output-path [service-name]
  (service-script-file service-name "nohup.out"))

(defn service-script-failed-path [service-name]
  (service-script-file service-name "nohup.failed"))

(defmethod service-supervisor-available? :nohup
  [_]
  true)

(defmethod service-supervisor-config :nohup
  [_ {:keys [service-name run-file user] :as config} _]
  (directory (service-script-file service-name) :owner user)
  (apply-map
   remote-file
   (service-script-path service-name)
   :mode "0755"
   run-file))

(defn start-nohup-service [service-name user]
  (actions/file (service-script-failed-path service-name) :action :delete)
  (with-action-options {:sudo-user user
                        :script-dir (service-script-path service-name)}
    (exec-checked-script
     (str "Start " service-name " via nohup")
     ("("
      (chain-or
       ("nohup" ~(service-script-path service-name)
        ">" (service-script-output-path ~service-name))
       ("touch" (service-script-failed-path ~service-name)))
      "&" ")")
     ("sleep" 5)
     (not (file-exists? (service-script-failed-path ~service-name))))))

(defn stop-nohup-service [service-name user]
  (with-action-options {:sudo-user user}
    (exec-checked-script
     (str "Kill " service-name " via killall")
     ("killall" ~service-name))))

(defmethod service-supervisor :nohup
  [_ {:keys [service-name user nohup]}
   {:keys [action if-flag if-stopped instance-id]
    :or {action :start}
    :as options}]
  (let [{:keys [process-name] :or {process-name service-name}} nohup
        action-arg (if (= :stop action) process-name service-name)]
    (if (#{:enable :disable :start-stop} action)
      (warnf "Requested action %s on service %s not implemented via nohup"
             action service-name)
      (if if-flag
        (plan-when (target-flag? if-flag)
          (exec-checked-script
           (str ~(name action) " " ~service-name " if config changed")
           (~(service-script-path service-name) ~(name action))))
        (if if-stopped
          (case
              :start (plan-when-not
                         (fragment (pipe ("ps") ("grep" ~service-name)))
                       (start-nohup-service service-name user))
              :stop nil
              :restart (plan-when-not
                           (fragment (pipe ("ps") ("grep" ~service-name)))
                         (stop-nohup-service action-arg user)
                         (start-nohup-service service-name user)))
          (case action
            :start (start-nohup-service service-name user)
            :stop (stop-nohup-service action-arg user)
            :restart (do
                       (stop-nohup-service action-arg user)
                       (start-nohup-service service-name user))))))))
