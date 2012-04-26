(ns pallet.core.api
  "Base level API for pallet"
  (:require
   [clojure.tools.logging :as logging])
  (:use
   [clojure.algo.monads :only [domonad m-map state-m with-monad]]
   [pallet.action-plan :only [execute stop-execution-on-error translate]]
   [pallet.compute :only [destroy-nodes-in-group destroy-node nodes run-nodes]]
   [pallet.core :only [default-executor]]
   [pallet.environment :only [get-for]]
   [pallet.session.action-plan
    :only [assoc-action-plan get-session-action-plan]]
   [pallet.session.verify :only [add-session-verification-key]]
   pallet.core.api-impl
   [slingshot.slingshot :only [throw+]]))

(defn service-state
  "Query the available nodes in a `compute-service`, filtering for nodes in the
  specified `groups`. Returns a map that contains all the nodes, nodes for each
  group, and groups for each node.

  Also the service environment."
  [compute-service groups]
  (let [nodes (nodes compute-service)
         ]
    {:node->groups (into {} (map (node->groups groups) nodes))
     :group->nodes (into {} (map (group->nodes nodes) groups))}))

;;; ## Action Plan Building
(defn action-plan
  "Build the action plan for the specified `plan-fn` on the given `node`, within
  the context of the `service-state`. The `plan-state` contains all the
  settings, etc, for all groups."
  [service-state environment plan-fn target]
  (fn action-plan [plan-state]
    (with-script-for-node (-> target :server :node)
      (let [session (add-session-verification-key
                     (merge
                      target
                      {:service-state service-state
                       :plan-state plan-state}))
            [rv session] (plan-fn session)
            [action-plan session] (translate (:action-plan session) session)]
        [action-plan (:plan-state session)]))))

(defn- action-plans-for-nodes
  [service-state environment phase group nodes]
  (if-let [plan-fn (-> group :phases phase)]
    (with-monad state-m
      (domonad
       [action-plans (m-map
                      #(action-plan
                        service-state environment plan-fn {:server {:node %}})
                      nodes)]
       (zipmap nodes action-plans)))
    (fn [plan-state] [nil plan-state])))

(defmulti action-plans
  "Build action plans for the specified `phase` on all nodes or groups in the
  given `target`, within the context of the `service-state`. The `plan-state`
  contains all the settings, etc, for all groups."
  (fn [service-state environment phase target-type target] target-type))

;; Build action plans for the specified `phase` on all nodes in the given
;; `group`, within the context of the `service-state`. The `plan-state` contains
;; all the settings, etc, for all groups.
(defmethod action-plans :group-nodes
  [service-state environment phase target-type group]
  (action-plans-for-nodes
   service-state environment phase group
   (-> service-state :group->nodes (get group))))

;; Build action plans for the specified `phase` on all nodes in the given
;; `group`, within the context of the `service-state`. The `plan-state` contains
;; all the settings, etc, for all groups.
(defmethod action-plans :group-node-list
  [service-state environment phase target-type [group nodes]]
  (action-plans-for-nodes service-state environment phase group nodes))

;; Build an action plan for the specified `phase` on the given `group`, within
;; the context of the `service-state`. The `plan-state` contains all the
;; settings, etc, for all groups.
(defmethod action-plans :group
  [service-state environment phase target-type group]
  (if-let [plan-fn (-> group :phases phase)]
    (with-monad state-m
      (domonad
       [action-plan (action-plan
                     service-state environment plan-fn {:group group})]
       action-plan))
    (fn [plan-state] [nil plan-state])))

(defn action-plans-for-phase
  "Build action plans for the specified `phase` on the given `groups`, within
  the context of the `service-state`. The `plan-state` contains all the
  settings, etc, for all groups.

  The exact format of `groups` depends on `target-type`.

  `target-type`
  : specifies the type of target to run the phase on, :group, :group-nodes,
  or :group-node-list."
  [service-state environment target-type targets phase]
  (with-monad state-m
    (domonad
     [action-plans (m-map
                    (partial
                     action-plans
                     service-state environment phase target-type)
                    targets)]
     (apply merge-with comp action-plans))))

(defn action-plans-for-phases
  "Build action plans for the specified `phase` on the given `groups`, within
  the context of the `service-state`. The `plan-state` contains all the
  settings, etc, for all groups."
  [target-type service-state environment groups phases]
  (logging/debugf
   "groups %s phases %s" (vec (map :group-name groups)) (vec phases))
  (with-monad state-m
    (domonad
     [action-plans (m-map
                    (partial
                     action-plans-for-phase
                     target-type service-state environment groups)
                    phases)]
     (apply merge-with comp action-plans))))


;;; ## Action Plan Execution
(defmulti session-target-map
  (fn [target-type target]
    target-type))

(defmethod session-target-map :node
  [target-type target]
  {:server {:node target}})

(defmethod session-target-map :group
  [target-type target]
  {:group target})

(defn execute-action-plan
  "Execute the `action-plan` on the `node`."
  [service-state plan-state environment action-plan target-type target]
  (with-script-for-node target
    (let [executor (get-in environment [:algorithms :executor] default-executor)
          execute-status-fn (get-in environment [:algorithms :execute-status-fn]
                                    #'stop-execution-on-error)
          session (merge
                   (session-target-map target-type target)
                   {:service-state service-state
                    :plan-state plan-state
                    :user pallet.utils/*admin-user*})
          [result session] (execute
                            action-plan session executor execute-status-fn)]
      (logging/debugf
       "execute-action-plan returning %s" [(:plan-state session) result])
      [(:plan-state session) result])))

;; (defn execute-action-plan-for-group
;;   "Execute the `action-plan` on the `group`."
;;   [service-state plan-state environment action-plan group]
;;   (let [executor (get-in environment [:algorithms :executor] default-executor)
;;         execute-status-fn (get-in environment [:algorithms :execute-status-fn]
;;                                   #'stop-execution-on-error)
;;         session {:service-state service-state
;;                  :group group
;;                  :plan-state plan-state
;;                  :user pallet.utils/*admin-user*}
;;         [result session] (execute
;;                           action-plan session executor execute-status-fn)]
;;     [(:plan-state session) result]))

(defn group-delta
  "Calculate actual and required counts for a group"
  [service-state group]
  (let [existing-count (count (-> service-state :group->nodes (get group)))
        target-count (:count group ::not-specified)]
    (when (= target-count ::not-specified)
      (throw+
       {:reason :target-count-not-specified
        :group group}
       "Node :count not specified for group: %s" (:group-name group)))
    {:actual existing-count :target target-count
     :delta (- target-count existing-count)}))

(defn group-deltas
  "Calculate actual and required counts for a sequence of groups. Returns a map
  from group to a map with :actual and :target counts."
  [service-state groups]
  (into
   {}
   (map
    (juxt identity (partial group-delta service-state))
    groups)))

(defn groups-to-create
  "Return a sequence of groups that currently have no nodes, but will have nodes
  added."
  [group-deltas]
  (letfn [(new-group? [{:keys [actual target]}]
            (and (zero? actual) (pos? target)))]
    (filter #(when (new-group? (second %)) (first %)) group-deltas)))

(defn groups-to-remove
  "Return a sequence of groups that will have nodes, but will have all nodes
  removed."
  [group-deltas]
  (letfn [(remove-group? [{:keys [actual target]}]
            (and (zero? target) (pos? actual)))]
    (filter #(when (remove-group? (second %)) (first %)) group-deltas)))

(defn nodes-to-remove
  "Finds the specified number of nodes to be removed from the given groups.
  Nodes are selected at random. Returns a map from group to a map with
  :servers and :all, where :servers is a sequence of severs to remove, and :all
  is a boolean that is true if all nodes are being removed."
  [service-state group-deltas]
  (letfn [(pick-servers [[group {:keys [delta target]}]]
            (vector
             group
             {:nodes (take (- delta) (-> service-state :group->nodes group))
              :all (zero? target)}))]
    (into {}
          (->>
           group-deltas
           (filter #(when (neg? (:delta (val %))) %))
           (map pick-servers)))))

(defn nodes-to-add
  "Finds the specified number of nodes to be added to the given groups.
  Returns a map from group to a count of servers to add"
  [group-deltas]
  (into {}
        (->>
         group-deltas
         (filter #(when (pos? (:delta (val %))) [(key %) (:delta (val %))])))))

;; (defn group-adjustments
;;   "Build a map with the various adjustments to make for the groups. This is a
;;   convenience for use in higher level code."
;;   [service-state groups]
;;   (let [group-deltas (group-deltas service-state groups)
;;         groups-to-create (groups-to-create group-deltas)
;;         groups-to-remove (groups-to-remove group-deltas)
;;         nodes-to-add (nodes-to-add group-deltas)
;;         nodes-to-remove (nodes-to-remove service-state group-deltas)]
;;     {:group-deltas group-deltas
;;      :groups-to-create groups-to-create
;;      :groups-to-remove groups-to-remove
;;      :nodes-to-add nodes-to-add
;;      :nodes-to-remove nodes-to-remove}))


(defn create-nodes
  "Create `count` nodes for a `group`."
  [compute-service environment group count]
  (run-nodes
   compute-service group count
   (get-for environment [:provider-options] nil)))

(defn remove-nodes
  "Removes `nodes` from `group`. If `all` is true, then all nodes for the group
  are being removed."
  [compute-service group {:keys [nodes all]}]
  (if all
    (destroy-nodes-in-group compute-service (name (:group-name group)))
    (doseq [node nodes] (destroy-node compute-service node))))




;; (defn adjust-node-count
;;   "Adjust actual and required count for a group"
;;   [compute-service {:keys [delta]} group]
;;   (cond
;;     (pos? delta) (create-nodes group delta)
;;     (neg? delta) (destroy-nodes group delta)))

;; (reduce
;;  (fn [{:keys plan-state action-plans} node]
;;    (let [[plan-state action-plan] (action-plan
;;                                    service-state plan-state environment
;;                                    node plan-fn)]
;;      {:plan-state plan-state
;;       :action-plans (conj action-plans action-plan)}))
;;  {:plan-state plan-state :action-plans []}
;;  (nodes-in-group service-state group))
