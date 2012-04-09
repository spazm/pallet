(ns pallet.session.action-plan
  "Session specific action-plan functions.

Encapsulates the location of the action-plans in the session.")

(defn get-action-plan
  "Set the session action-plan"
  [session]
  (::action-plan session))

(defn assoc-action-plan
  "Set the session action-plan"
  [session action-plan]
  (assoc session ::action-plan action-plan))

(defn get-session-action-plan
  "Retrieves the current action plan, and resets it"
  [session]
  {:pre [(map? session)]}
  [(::action-plan session) (dissoc session ::action-plan)])

(defn- target-path*
  "Return the vector path of the action plan for the specified phase an
  target-id."
  [phase target-id]
  [:action-plan phase target-id])

(defn target-path
  "Return the vector path of the action plan for the current session target
   node, or target group."
  [session]
  {:pre [(keyword? (:phase session))
         (keyword? (:target-id session))]}
  (target-path* (:phase session) (-> session :target-id)))

;; (defn mv-session-action-plan
;;   "Move the session action-plan into it's target specific location"
;;   [session]
;;   (let [action-plan (::action-plan session)]
;;     [action-plan
;;      (->
;;       session
;;       (assoc-in (target-path session) action-plan)
;;       (dissoc ::action-plan))]))

(defn update-action-plan
  "Session action plan update applier"
  [session f]
  (update-in session [::action-plan] f))