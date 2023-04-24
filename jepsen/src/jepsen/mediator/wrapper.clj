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

(defn start-mediator-time
  "Lets the mediator know that relative time has started."
  []
  (let [ts (util/origin-time)
        options {:form-params ts}]
    @(http-kit-client/post (control-endpoint "/test/start_time") options)))

(defn about-to-tear-down-db
  "Lets the mediator know that the DB is going to be teared down."
  []
  @(http-kit-client/post (control-endpoint "/test/before_tear_down")))