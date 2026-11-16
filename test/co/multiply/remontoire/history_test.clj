(ns co.multiply.remontoire.history-test
  (:require
    [clojure.test :refer [deftest is use-fixtures]]
    [co.multiply.remontoire.api :as api]
    [co.multiply.remontoire.history :as history])
  (:import
    (java.nio.charset StandardCharsets)
    (java.util.concurrent.atomic AtomicLong)
    (java.util.concurrent.locks ReentrantLock)))


(defn isolated-history
  [f]
  ;; Isolate the store even when these tests are loaded into a live host.
  (with-redefs-fn {#'history/state (atom @@#'history/state)
                  #'history/writer (ReentrantLock.)
                  #'history/gaps (AtomicLong.)}
    #(do (api/nrepl-capture! {:enabled? true :max-entries 200
                             :entry-bytes 65536 :total-bytes 8388608})
         (api/clear-nrepl-history!)
         (f))))


(use-fixtures :each isolated-history)


(defn- begin
  [session input]
  (history/begin! {:op "eval" :session session :id (str (random-uuid))
                   :ns "user" :code input}))


(deftest summaries-and-transcripts-are-separate
  (let [a (begin "editor-a" "(+ 1 2)")
        b (begin "editor-b" "(/ 1 0)")]
    (history/response! a {:out "hello\n"})
    (history/response! a {:value "3" :ns "user"})
    (history/response! a {:status #{:done}})
    (history/response! b {:status #{:eval-error} :ex "ArithmeticException"})
    (is (= :running (:status (api/nrepl-transcript b))) "failure before terminal response")
    (history/response! b {:status #{:done}})
    (is (= [b a] (mapv :id (:entries (api/nrepl-history)))))
    (is (every? #(not-any? (set (keys %)) [:input :events])
          (:entries (api/nrepl-history))))
    (is (= [a] (mapv :id (:entries (api/nrepl-history {:session "editor-a"})))))
    (is (= [b] (mapv :id (:entries (api/nrepl-history {:status :failed})))))
    (is (= [a] (mapv :id (:entries (api/nrepl-history {:before b :limit 1})))))
    (is (empty? (:entries (api/nrepl-history {:since Long/MAX_VALUE}))))
    (is (= {:input "(+ 1 2)" :status :done :session "editor-a"}
          (select-keys (api/nrepl-transcript a) [:input :status :session])))
    (is (= ["hello\n"] (vec (keep :out (:events (api/nrepl-transcript a))))))
    (is (= ["3"] (vec (keep :value (:events (api/nrepl-transcript a))))))))


(deftest count-limits-do-not-protect-running-or-refresh-read-entries
  (api/nrepl-capture! {:max-entries 2})
  (let [a (begin "a" "1")
        b (begin "a" "2")]
    (api/nrepl-transcript a)
    (let [c (begin "a" "3")]
      (is (= :evicted (:missing (api/nrepl-transcript a))))
      (history/response! a {:value "late" :status #{:done}})
      (is (= [c b] (mapv :id (:entries (api/nrepl-history)))))
      (is (= a (get-in (api/nrepl-history) [:capture :evicted-through]))))))


(deftest byte-limits-bound-large-input-output-and-tiny-event-floods
  (api/nrepl-capture! {:entry-bytes 1024 :total-bytes 2048})
  (let [a (begin "a" (apply str (repeat 5000 "😀")))
        transcript (api/nrepl-transcript a)]
    (is (:truncated? transcript))
    (is (<= (:bytes transcript) 1024))
    (is (even? (count (:input transcript))) "no split surrogate pairs")
    (is (= (:input transcript)
          (String. (String/.getBytes (:input transcript) StandardCharsets/UTF_8)
            StandardCharsets/UTF_8)))
    (history/response! a {:out (apply str (repeat 10000 "x"))})
    (history/response! a {:status #{:done}})
    (is (= :done (:status (api/nrepl-transcript a))) "terminal state survives text truncation"))
  (let [b (begin "a" "2")]
    (dotimes [_ 2000] (history/response! b {:out ""}))
    (is (<= (count (:events (api/nrepl-transcript b))) 16))
    (is (:truncated? (api/nrepl-transcript b))))
  (dotimes [_ 10] (begin "a" (apply str (repeat 1000 "x"))))
  (is (<= (:bytes (api/nrepl-capture!)) 2048))
  (is (<= (:entries (api/nrepl-capture!)) 2)))


(deftest clear-and-pause-have-distinct-semantics
  (let [a (begin "a" "1")]
    (api/nrepl-capture! {:enabled? false})
    (is (nil? (begin "a" "2")))
    (history/response! a {:value "hidden" :status #{:done}})
    (is (= :unknown (:status (api/nrepl-transcript a))))
    (is (:incomplete? (api/nrepl-transcript a)))
    (api/nrepl-capture! {:enabled? true})
    (is (empty? (:events (api/nrepl-transcript a))))
    (api/clear-nrepl-history!)
    (is (:enabled? (api/nrepl-capture!)))
    (history/response! a {:value "late"})
    (is (= :cleared (:missing (api/nrepl-transcript a))))
    (is (> (begin "a" "3") a))
    (is (= :unknown (:missing (api/nrepl-transcript Long/MAX_VALUE))))))


(deftest lowered-limits-and-validation
  (begin "a" (apply str (repeat 2000 "x")))
  (api/nrepl-capture! {:entry-bytes 512})
  (is (zero? (:entries (api/nrepl-capture!))))
  (doseq [opts [{:enabled? :yes} {:max-entries 0} {:entry-bytes 10}
               {:total-bytes -1} {:max-entries 1.5} {:typo 10}]]
    (is (thrown? clojure.lang.ExceptionInfo (api/nrepl-capture! opts))))
  (is (thrown? clojure.lang.ExceptionInfo (api/nrepl-history {:limit -1})))
  (is (thrown? clojure.lang.ExceptionInfo (api/nrepl-transcript "1"))))


(deftest lowering-limits-while-pausing-preserves-retention-order
  (let [old-small (begin "a" "1")
        old-large (begin "a" (apply str (repeat 1000 "x")))
        middle-small (begin "a" "2")
        newest-large (begin "a" (apply str (repeat 1000 "x")))
        running (begin "a" "3")
        done (begin "a" "4")]
    (history/response! done {:status #{:done}})
    (api/nrepl-capture! {:enabled? false :entry-bytes 768})
    (is (= [done running] (mapv :id (:entries (api/nrepl-history)))))
    (doseq [id [old-small old-large middle-small newest-large]]
      (is (= :evicted (:missing (api/nrepl-transcript id)))))
    (is (= newest-large (:evicted-through (api/nrepl-capture!))))
    (is (= :unknown (:status (api/nrepl-transcript running))))
    (is (:incomplete? (api/nrepl-transcript running)))
    (is (= :done (:status (api/nrepl-transcript done))))
    (is (not (:incomplete? (api/nrepl-transcript done))))
    (is (= (+ (:bytes (api/nrepl-transcript running)) (:bytes (api/nrepl-transcript done)))
          (:bytes (api/nrepl-capture!))))
    ;; Count limits still apply after oversized entries have been removed.
    (let [pauses (:pauses (api/nrepl-capture!))]
      (api/nrepl-capture! {:enabled? false :max-entries 1})
      (is (= pauses (:pauses (api/nrepl-capture!))))
      (is (= [done] (mapv :id (:entries (api/nrepl-history)))))
      (is (= :evicted (:missing (api/nrepl-transcript running)))))
    (api/nrepl-capture! {:enabled? true})
    (is (= :done (:status (api/nrepl-transcript done))))))


(deftest invalid-configuration-does-not-pause-or-evict
  (let [id (begin "a" (apply str (repeat 1000 "x")))
        before (api/nrepl-capture!)
        transcript (api/nrepl-transcript id)]
    (is (thrown? clojure.lang.ExceptionInfo
          (api/nrepl-capture! {:enabled? false :entry-bytes 256 :max-entries 0})))
    (is (= before (api/nrepl-capture!)))
    (is (= transcript (api/nrepl-transcript id)))
    (history/response! id {:status #{:done}})
    (is (= :done (:status (api/nrepl-transcript id))))))


(deftest arbitrary-values-are-not-printed-realized-or-retained
  (let [touched (atom false)
        value (lazy-seq (reset! touched true) (throw (Exception. "must not realize")))
        a (begin "a" "1")]
    (history/response! a {:value value :nrepl.middleware.caught/throwable (Exception.)})
    (is (false? @touched))
    (is (:incomplete? (api/nrepl-transcript a)))
    (is (not-any? :value (:events (api/nrepl-transcript a))))))


(deftest contention-skips-capture-instead-of-waiting
  (let [a (begin "a" "1")
        acquired (promise)
        release (promise)
        holder (Thread. ^Runnable
                 (fn []
                   (ReentrantLock/.lock @#'history/writer)
                   (try (deliver acquired true) @release
                        (finally (ReentrantLock/.unlock @#'history/writer)))))]
    (Thread/.start holder)
    (try
      (is (= true (deref acquired 2000 :timeout)))
      (let [result (promise)
            recorder (Thread. ^Runnable #(deliver result (begin "a" "2")))]
        (Thread/.start recorder)
        (is (nil? (deref result 2000 :timeout)) "completes while the other writer holds the lock")
        (Thread/.join recorder 2000))
      (is (pos? (:capture-gaps (api/nrepl-capture!))))
      (is (:capture-gaps? (api/nrepl-transcript a)))
      (is (= :unknown (:status (api/nrepl-transcript a))))
      (finally
        (deliver release true)
        (Thread/.join holder 2000)))))


(deftest interruption-matches-session-as-well-as-request-id
  (let [a (history/begin! {:op "eval" :session "a" :id "same" :code "1"})
        b (history/begin! {:op "eval" :session "b" :id "same" :code "2"})]
    (history/interrupt! {:session "a" :id "same" :status #{:interrupted :done}})
    (is (= :interrupted (:status (api/nrepl-transcript a))))
    (is (= :running (:status (api/nrepl-transcript b))))
    (history/response! a {:status #{:done}})
    (is (= :interrupted (:status (api/nrepl-transcript a))))))


(deftest status-details-are-preserved-without-interning-names
  (let [a (begin "a" "1")]
    (history/response! a {:status #{:error :namespace-not-found :done}})
    (is (= #{"error" "namespace-not-found" "done"}
          (set (:status (first (:events (api/nrepl-transcript a))))))))
  (let [a (begin "a" "2")]
    (history/response! a {:status ["custom/failure" :done]})
    (is (= ["custom/failure" "done"]
          (:status (first (:events (api/nrepl-transcript a))))))))


(deftest pause-gaps-remain-discoverable-after-resume
  (let [before (:pauses (api/nrepl-capture!) 0)]
    (api/nrepl-capture! {:enabled? false})
    (api/nrepl-capture! {:enabled? true})
    (let [capture (:capture (api/nrepl-history))]
      (is (= (inc before) (:pauses capture)))
      (is (int? (:last-paused-at capture)))
      (is (<= (:last-paused-at capture) (:last-enabled-at capture))))))


(deftest upstream-print-truncation-is-visible
  (let [a (begin "a" "1")]
    (history/response! a {:status :nrepl.middleware.print/truncated})
    (history/response! a {:status :done})
    (is (:incomplete? (api/nrepl-transcript a)))))
