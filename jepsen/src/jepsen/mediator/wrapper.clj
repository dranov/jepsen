(ns jepsen.mediator.wrapper
  (:require [clojure [core :as c]]
            [clojure.tools.logging :refer [info warn]]
            [org.httpkit.client :as http-kit-client]
            [jepsen
             [util :as util]]
            [jepsen.store :as store]
            [slingshot.slingshot :refer [try+ throw+]]))

(def control-url "http://127.0.0.1:5000")
(defn control-endpoint [endpoint] (str control-url endpoint))

(defn inform-mediator
  "Lets the mediator know that a new test has started or ended.
   event = :start or :end."
  [test event]
  (let [options {:form-params
                 {:nodes (str (:nodes test))
                  :start_time (:start-time test)
                  :total_count (:test-count test)
                  :exploration_count (:exploration-count test)
                  :store_path (.getCanonicalPath (store/path! test))}}
        url (case event
              :start (control-endpoint "/test/start")
              :end (control-endpoint "/test/end"))
        {:keys [_ error]} @(http-kit-client/post url options)]
    (if error
      (warn "Error contacting the mediator. Error: " error)
      (info "Succesfully established contact with the mediator.")))
  test)

(defn noop-nemesis
  "Set up a noop nemesis on the mediator."
  []
  (let [nem {:enabled false
             :op-types []
             :enabled-op-types []
             :enabled-ops []
             :reset-ops []}
        nemesis-config {:nemeses [] :opts {}}
        options {:form-params {:nemesis (pr-str nemesis-config)}}]
    @(http-kit-client/post (control-endpoint "/nemesis/setup") options)))

(defn start-mediator-time
  "Lets the mediator know that relative time has started."
  []
  (let [ts (util/origin-time)
        options {:form-params ts}]
    @(http-kit-client/post (control-endpoint "/test/start_time") options)
    ;; Also set-up a noop nemesis
    (noop-nemesis)))

(defn about-to-tear-down-db
  "Lets the mediator know that the DB is going to be teared down."
  []
  @(http-kit-client/post (control-endpoint "/test/before_tear_down")))

(defn nemesis-request-op
  "Requests an operation from the mediator nemesis. We don't use the result
   in passive mode, but it is still needed to advance the mediator's time."
  []
  @(http-kit-client/post (control-endpoint "/nemesis/op")))

(defn inform-invoke-op
  "Lets the mediator know that we have invoked an operation."
  [op]
  (let [options {:form-params {:op (pr-str op)}}]
    @(http-kit-client/post (control-endpoint "/client/invoke") options)
    (nemesis-request-op)))

(defn inform-complete-op
  "Lets the mediator know that we have completed an operation."
  [op]
  (let [options {:form-params {:op (pr-str op)}}]
    @(http-kit-client/post (control-endpoint "/client/complete") options)))