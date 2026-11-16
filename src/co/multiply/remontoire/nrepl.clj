(ns co.multiply.remontoire.nrepl
  "Optional nREPL middleware for transcript capture. Hosts supply nREPL; neither
   Remontoire startup nor its eval-facing toolkit loads this namespace."
  (:require
    [co.multiply.remontoire.history :as history]
    [nrepl.middleware :as middleware]
    [nrepl.middleware.load-file :as load-file]
    [nrepl.middleware.print :as print]
    [nrepl.middleware.session :as session]
    [nrepl.transport :as transport]))


(defn- observe
  [f]
  (try (f) (catch Throwable _ (history/gap!))))


(defn wrap-history
  "Observe eval/load-file inputs and printed outgoing responses. Install this
   var via nREPL's middleware stack AND enable capture with
   `co.multiply.remontoire.api/nrepl-capture!`. Installation alone is inert.

   Completion, lookup, and other non-eval operations aren't history entries.
   Clients may suppress an automated eval with ::record? false (0 over bencode).
   Unmarked evals are indistinguishable from human input and are captured.

   Interrupt responses travel on a different request's transport; observe them
   too, correlating by original request id and session. Never wrap evaluation,
   print values ourselves, retain request maps, or suppress transport failures."
  [handler]
  (fn [{:keys [op transport] :as msg}]
    (let [record? (not (contains? #{false 0 "false"} (::record? msg)))
          history-id (when (and record? (#{"eval" "load-file"} op))
                       (observe #(history/begin! msg)))]
      (if-not (or history-id (= op "interrupt"))
        (handler msg)
        (let [wrapped (reify transport/Transport
                        (recv [_] (transport/recv transport))
                        (recv [_ timeout] (transport/recv transport timeout))
                        (send [this response]
                          (observe #(if history-id
                                      (history/response! history-id response)
                                      (history/interrupt! response)))
                          (transport/send transport response)
                          this))]
          (try
            (handler (assoc msg :transport wrapped))
            (catch Throwable e
              (when history-id
                (observe #(history/response! history-id
                            {:status #{:error :done}
                             :ex (Class/.getName (Object/.getClass e))})))
              (throw e))))))))


;; Outer to session resolution, printing and load-file rewriting on ingress;
;; on egress this sees strings after printing and only load-file's final value.
(middleware/set-descriptor! #'wrap-history
  {:requires #{}
   :expects #{#'session/session #'print/wrap-print #'load-file/wrap-load-file}
   :handles {}})
