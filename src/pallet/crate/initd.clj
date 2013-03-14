(ns pallet.crate.initd
  "Provides service supervision via initd"
  (:require
   [pallet.actions :refer [exec-checked-script remote-file]]
   [pallet.actions.direct.service :refer [service-impl]]
   [pallet.core.session :refer [session]]
   [pallet.crate.service
    :refer [service-supervisor service-supervisor-available?
            service-supervisor-config]]
   [pallet.script.lib :refer [file etc-init]]
   [pallet.stevedore :refer [fragment]]
   [pallet.utils :refer [apply-map]]))

(defn init-script-path
  "Return the init script path for the given service name."
  [service-name]
  (fragment (file (etc-init) ~service-name)))

(defmethod service-supervisor-available? :initd
  [_]
  true)

(defmethod service-supervisor-config :initd
  [_ {:keys [service-name init-file] :as config} _]
  (apply-map
   remote-file
   (init-script-path service-name)
   :owner "root" :group "root" :mode "0755"
   init-file))

(defmethod service-supervisor :initd
  [_ {:keys [service-name]} {:keys [action if-flag if-stopped instance-id]
                             :or {action :start}
                             :as options}]
  (exec-checked-script
   (str "Initd " (name action) " " service-name)
   ~(apply-map service-impl (session) :initd service-name options)))
