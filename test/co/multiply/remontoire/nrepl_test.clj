(ns co.multiply.remontoire.nrepl-test
  (:require
    [clojure.test :refer [deftest is use-fixtures]]
    [co.multiply.remontoire.api :as api]
    [co.multiply.remontoire.history :as history]
    [co.multiply.remontoire.history-test :refer [isolated-history]]
    [co.multiply.remontoire.nrepl :as recorder]
    [nrepl.core :as nrepl]
    [nrepl.server :as server]
    [nrepl.transport :as transport]))


(use-fixtures :each isolated-history)


(defn- with-server
  [f]
  (with-open [server (server/start-server :port 0 :bind "127.0.0.1"
                      :handler (server/default-handler #'recorder/wrap-history))
              connection (nrepl/connect :port (:port server))]
    (let [client (nrepl/client connection 5000)
          session (:new-session (first (doall (nrepl/message client {:op "clone"}))))]
      (try (f client session connection)
           (finally (doall (nrepl/message client {:op "close" :session session})))))))


(defn- request
  [client session msg]
  (vec (nrepl/message client (assoc msg :session session))))


(defn- latest
  []
  (some-> (api/nrepl-history {:limit 1}) :entries first :id api/nrepl-transcript))


(deftest real-stack-preserves-client-output-and-captures-printed-values
  (with-server
    (fn [client session _]
      (let [code "(println \"out\") (binding [*out* *err*] (println \"err\")) (+ 1 2)"
            responses (request client session {:op "eval" :id "human-eval" :code code})
            recorded (latest)]
        (is (= :done (:status recorded)))
        (is (= session (:session recorded)))
        (is (= "human-eval" (:request-id recorded)))
        (is (= code (:input recorded)))
        (doseq [key [:value :out :err]]
          (is (= (vec (keep key responses)) (vec (keep key (:events recorded))))))
        (is (= "3" (last (keep :value (:events recorded)))))))))


(def printer-calls (atom 0))


(defn counting-printer
  [_ writer _]
  (swap! printer-calls inc)
  (java.io.Writer/.write writer "custom-printed-value"))


(deftest configured-printer-is-used-exactly-once
  (with-redefs [printer-calls (atom 0)]
    (with-server
      (fn [client session _]
        (let [responses (request client session
                          {:op "eval" :code "{:answer 42}"
                           :nrepl.middleware.print/print
                           "co.multiply.remontoire.nrepl-test/counting-printer"})]
          (is (= 1 @printer-calls))
          (is (= ["custom-printed-value"] (vec (keep :value responses))
                (vec (keep :value (:events (latest)))))))))))


(deftest load-file-is-one-original-request-with-only-the-final-result
  (with-server
    (fn [client session _]
      (let [code "(ns history.example)\n(+ 1 1)\n(+ 2 2)"
            responses (request client session {:op "load-file" :file code
                                               :file-path "history/example.clj"
                                               :file-name "example.clj"})
            recorded (latest)]
        (is (= 1 (count (:entries (api/nrepl-history)))))
        (is (= "load-file" (:op recorded)))
        (is (= code (:input recorded)))
        (is (= "history/example.clj" (:file recorded)))
        (is (= ["4"] (vec (keep :value responses))
              (vec (keep :value (:events recorded))))))
      (remove-ns 'history.example))))


(deftest protocol-errors-and-eval-failures-remain-visible
  (with-server
    (fn [client session _]
      (request client session {:op "eval" :code "(/ 1 0)"})
      (is (= :failed (:status (latest))))
      (is (some :ex (:events (latest))))
      (is (some :err (:events (latest))))
      (request client session {:op "eval" :ns "no.such.history.namespace" :code "1"})
      (is (= :failed (:status (latest))))
      (request client "no-such-session" {:op "eval" :code "1"})
      (is (= :failed (:status (latest)))))))


(deftest printer-runs-only-once-and-streamed-chunks-are-preserved
  (with-server
    (fn [client session _]
      ;; nREPL itself realizes the lazy result. Capturing may not do so again.
      (let [responses (request client session
                        {:op "eval"
                         :code "(map (fn [x] (println \"realized\") x) (range 3))"
                         :nrepl.middleware.print/stream? 1
                         :nrepl.middleware.print/buffer-size 2})]
        (is (= (vec (keep :value responses)) (vec (keep :value (:events (latest))))))
        (is (= "(0 1 2)" (apply str (keep :value responses))))
        (is (= 3 (count (re-seq #"realized" (apply str (keep :out responses)))))))
      (request client session {:op "eval" :code "(range 100)"
                               :nrepl.middleware.print/stream? 1
                               :nrepl.middleware.print/quota 10})
      (is (:incomplete? (latest))))))


(deftest completion-and-explicitly-suppressed-evals-do-not-enter-history
  (with-server
    (fn [client session _]
      (request client session {:op "describe"})
      (request client session {:op "lookup" :sym "map" :ns "clojure.core"})
      (request client session {:op "completions" :prefix "ma" :ns "clojure.core"})
      (let [responses (request client session {:op "eval" :code "(+ 1 2)"
                                               :co.multiply.remontoire.nrepl/record? 0})]
        (is (= ["3"] (vec (keep :value responses)))))
      (is (empty? (:entries (api/nrepl-history)))))))


(deftest paused-capture-does-not-affect-evaluation
  (api/nrepl-capture! {:enabled? false})
  (with-server
    (fn [client session _]
      (is (= ["3"] (vec (keep :value (request client session {:op "eval" :code "(+ 1 2)"})))))
      (is (empty? (:entries (api/nrepl-history))))
      (api/nrepl-capture! {:enabled? true})
      (request client session {:op "eval" :code "4"})
      (is (= "4" (:input (latest)))))))


(deftest running-output-and-interrupts-are-attributed-to-the-original-eval
  (with-server
    (fn [client session connection]
      (transport/send connection {:op "eval" :id "waiting" :session session
                                  :code "(do (println \"working\") (flush) (Thread/sleep 30000))"})
      (let [first-response (transport/recv connection 5000)]
        (is (= "working\n" (:out first-response)))
        (is (= :running (:status (latest))))
        (is (= ["working\n"] (vec (keep :out (:events (latest)))))))
      (request client session {:op "interrupt" :interrupt-id "waiting"})
      (is (= :interrupted (:status (latest))))
      (is (= "waiting" (:request-id (latest))))
      (is (= 1 (count (:entries (api/nrepl-history))))))))


(deftest failing-recorder-does-not-affect-the-real-client
  (with-server
    (fn [client session _]
      (with-redefs [history/begin! (fn [_] (throw (Exception. "capture failed")))]
        (is (= ["3"] (vec (keep :value (request client session {:op "eval" :code "3"}))))))
      (with-redefs [history/response! (fn [& _] (throw (Exception. "capture failed")))]
        (is (= ["4"] (vec (keep :value (request client session {:op "eval" :code "4"}))))))
      (is (pos? (:capture-gaps (api/nrepl-capture!))))
      (is (:capture-gaps? (latest))))))


(deftest boolean-opt-out-works-before-wire-encoding
  (let [called (atom false)
        handler (recorder/wrap-history (fn [_] (reset! called true)))]
    (handler {:op "eval" :code "1" :co.multiply.remontoire.nrepl/record? false})
    (is @called)
    (is (empty? (:entries (api/nrepl-history))))))


(deftest middleware-does-not-realize-or-print-unexpected-response-objects
  (let [touched (atom false)
        value (lazy-seq (reset! touched true) (throw (Exception. "do not realize")))
        sent (atom nil)
        wire (reify transport/Transport
               (recv [_] :recv)
               (recv [_ timeout] timeout)
               (send [this response] (reset! sent response) this))
        handler (recorder/wrap-history
                  (fn [{:keys [transport]}]
                    (is (= :recv (transport/recv transport)))
                    (is (= 123 (transport/recv transport 123)))
                    (transport/send transport {:value value :status #{:done}})))]
    (handler {:op "eval" :code "1" :transport wire})
    (is (identical? value (:value @sent)))
    (is (false? @touched))
    (is (:incomplete? (latest)))))


(deftest handler-and-transport-failures-are-not-swallowed
  (let [failure (Exception. "original failure")
        handler (recorder/wrap-history (fn [_] (throw failure)))]
    (is (identical? failure
          (try (handler {:op "eval" :code "1"})
               (catch Exception e e))))
    (is (= :failed (:status (latest)))))
  (let [failure (Exception. "transport failure")
        wire (reify transport/Transport
               (recv [_] nil)
               (recv [_ _] nil)
               (send [_ _] (throw failure)))
        handler (recorder/wrap-history
                  (fn [{:keys [transport]}] (transport/send transport {:value "1"})))]
    (is (identical? failure
          (try (handler {:op "eval" :code "1" :transport wire})
               (catch Exception e e))))))
