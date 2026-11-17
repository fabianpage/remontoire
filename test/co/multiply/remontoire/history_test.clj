(ns co.multiply.remontoire.history-test
  (:require
    [clojure.string :as str]
    [clojure.test :refer [deftest is use-fixtures]]
    [co.multiply.remontoire.api :as api]
    [co.multiply.remontoire.history :as history]))


(defn isolated-history
  [f]
  (with-redefs-fn {#'history/state (atom @@#'history/state)}
    #(do (api/nrepl-capture! {:enabled? true})
         (api/clear-nrepl-history!)
         (f))))


(use-fixtures :each isolated-history)


(defn- begin
  [session input]
  (history/begin! {:op "eval" :session session :id (str (random-uuid)) :code input}))


(defn- code-points
  [s]
  (String/.codePointCount s 0 (count s)))


(deftest defaults-and-explicit-enablement
  (is (= {:enabled? false :max-entries 5 :max-chars 10000} @#'history/defaults))
  (api/nrepl-capture! {:enabled? false})
  (is (nil? (begin "a" "not recorded")))
  (is (empty? (:entries (api/nrepl-history))))
  (api/nrepl-capture! {:enabled? true})
  (is (int? (begin "a" "recorded"))))


(deftest summaries-and-transcripts-are-separate
  (let [a (begin "editor-a" "(+ 1 2)")
        b (begin "editor-b" "(/ 1 0)")]
    (history/response! a {:out "hello"})
    (history/response! a {:value "3" :ns "user"})
    (history/response! b {:err "division failed" :ex "ArithmeticException"})
    (history/response! b {:status #{:eval-error :done}})
    (is (= [b a] (mapv :id (:entries (api/nrepl-history)))))
    (is (every? #(not-any? (set (keys %)) [:input :output :events :status])
          (:entries (api/nrepl-history))))
    (is (= {:input "(+ 1 2)" :session "editor-a" :op "eval"}
          (select-keys (api/nrepl-transcript a) [:input :session :op])))
    (is (int? (:at (api/nrepl-transcript a))))
    (is (string? (:request-id (api/nrepl-transcript a))))
    (let [output (:output (api/nrepl-transcript a))]
      (is (str/includes? output "hello"))
      (is (str/includes? output "3"))
      (is (< (str/index-of output "hello") (str/index-of output "3"))))
    (is (str/includes? (:output (api/nrepl-transcript b)) "ArithmeticException"))
    (is (not-any? (set (keys (api/nrepl-transcript b))) [:status :events :bytes]))))


(deftest five-newest-starts-globally-even-when-older-work-is-running
  (let [ids (mapv #(begin (str "editor-" %) (str %)) (range 5))
        oldest (first ids)]
    (api/nrepl-transcript oldest)
    (history/response! oldest {:out "still running"})
    (let [newest (begin "another-editor" "sixth")]
      (is (= (vec (reverse (conj (subvec ids 1) newest)))
            (mapv :id (:entries (api/nrepl-history)))))
      (is (:missing (api/nrepl-transcript oldest)))
      (history/response! oldest {:value "late"})
      (is (:missing (api/nrepl-transcript oldest)))
      (is (= 5 (:entries (api/nrepl-capture!)))))))


(deftest input-prefix-and-output-tail-have-independent-code-point-limits
  (api/nrepl-capture! {:max-chars 10})
  (let [id (begin "a" (str (apply str (repeat 10 "😀")) "discarded"))]
    (history/response! id {:out (apply str (repeat 100 "x"))})
    (history/response! id {:err "終😀"})
    (let [{:keys [input output input-truncated? output-truncated?]} (api/nrepl-transcript id)]
      (is (= (apply str (repeat 10 "😀")) input))
      (is (= 10 (code-points input)))
      (is (= 10 (code-points output)))
      (is (str/includes? output "終😀"))
      (is (not (str/includes? output "x")))
      (is input-truncated?)
      (is output-truncated?))))


(deftest exact-input-limit-and-smallest-limit-preserve-surrogate-pairs
  (api/nrepl-capture! {:max-chars 1})
  (let [id (begin "a" "😀")]
    (is (= "😀" (:input (api/nrepl-transcript id))))
    (is (false? (:input-truncated? (api/nrepl-transcript id))))
    (is (false? (:output-truncated? (api/nrepl-transcript id)))))
  (let [id (begin "a" "😀x")]
    (is (= "😀" (:input (api/nrepl-transcript id))))
    (is (:input-truncated? (api/nrepl-transcript id)))))


(deftest huge-output-and-tiny-chunks-stay-bounded
  (api/nrepl-capture! {:max-chars 32})
  (let [id (begin "a" "code")]
    (history/response! id {:out (apply str (repeat 100000 "😀"))})
    (is (= 32 (code-points (:output (api/nrepl-transcript id)))))
    (dotimes [_ 2000] (history/response! id {:out "x"}))
    (is (= 32 (code-points (:output (api/nrepl-transcript id)))))
    (is (:output-truncated? (api/nrepl-transcript id)))
    (is (not (contains? (api/nrepl-transcript id) :events)))))


(deftest pause-keeps-snapshots-but-resume-only-records-new-evaluations
  (let [id (begin "a" "old")]
    (history/response! id {:out "before pause"})
    (let [before (api/nrepl-transcript id)]
      (api/nrepl-capture! {:enabled? false})
      (history/response! id {:out "during pause"})
      (is (nil? (begin "a" "started while paused")))
      (api/nrepl-capture! {:enabled? true})
      (history/response! id {:out "after resume"})
      (is (= before (api/nrepl-transcript id))))
    (let [new-id (begin "a" "new")]
      (history/response! new-id {:value "new result"})
      (is (str/includes? (:output (api/nrepl-transcript new-id)) "new result")))))


(deftest clear-does-not-reuse-ids-or-allow-late-resurrection
  (let [id (begin "a" "1")
        config (select-keys (api/nrepl-capture!) [:enabled? :max-entries :max-chars])]
    (api/clear-nrepl-history!)
    (history/response! id {:out "late"})
    (is (:missing (api/nrepl-transcript id)))
    (is (empty? (:entries (api/nrepl-history))))
    (is (= config (select-keys (api/nrepl-capture!) (keys config))))
    (is (> (begin "a" "2") id))
    (is (:missing (api/nrepl-transcript Long/MAX_VALUE)))))


(deftest lowering-limits-trims-immediately-without-evicting-oversized-survivors
  (let [a (begin "a" "oldest")
        b (begin "a" "abcdef")
        c (begin "a" "😀😀😀😀😀")]
    (history/response! b {:out "0123456789"})
    (api/nrepl-capture! {:max-entries 2 :max-chars 4})
    (is (:missing (api/nrepl-transcript a)))
    (is (= [c b] (mapv :id (:entries (api/nrepl-history)))))
    (is (= "abcd" (:input (api/nrepl-transcript b))))
    (is (= "😀😀😀😀" (:input (api/nrepl-transcript c))))
    (is (= 4 (code-points (:output (api/nrepl-transcript b)))))
    (is (str/includes? (:output (api/nrepl-transcript b)) "789"))
    (is (:input-truncated? (api/nrepl-transcript b)))
    (is (:output-truncated? (api/nrepl-transcript b)))
    (let [before (api/nrepl-transcript b)]
      (api/nrepl-capture! {:max-entries 5 :max-chars 10000})
      (is (= before (api/nrepl-transcript b)))
      (history/response! b {:out "new output"})
      (is (str/includes? (:output (api/nrepl-transcript b)) "new output"))
      (is (:output-truncated? (api/nrepl-transcript b))))))


(deftest invalid-configuration-has-no-side-effects
  (let [id (begin "a" "keep me")
        before (api/nrepl-history)]
    (doseq [opts [{:enabled? :yes} {:max-entries 0} {:max-chars 0}
                 {:max-chars -1} {:max-entries 1.5} {:max-chars nil}
                 {:enabled? false :max-entries 0} {:typo 10}
                 {:entry-bytes 1024} {:total-bytes 4096} nil]]
      (is (thrown? clojure.lang.ExceptionInfo (api/nrepl-capture! opts)))
      (is (= before (api/nrepl-history))))
    (history/response! id {:out "still recording"})
    (is (str/includes? (:output (api/nrepl-transcript id)) "still recording"))
    (doseq [id [0 -1 "1" nil]]
      (is (thrown? clojure.lang.ExceptionInfo (api/nrepl-transcript id))))))


(deftest arbitrary-values-are-not-printed-realized-or-retained
  (let [touched (atom false)
        value (lazy-seq (reset! touched true) (throw (Exception. "must not realize")))
        id (begin "a" "1")]
    (history/response! id {:value value :status value
                           :nrepl.middleware.caught/throwable (Exception.)})
    (is (false? @touched))
    (is (= "" (:output (api/nrepl-transcript id))))))


(deftest concurrent-writers-preserve-every-chunk-within-the-limit
  (let [id (begin "a" "1")
        start (promise)
        workers (mapv (fn [i]
                        (future @start
                                (dotimes [j 20]
                                  (history/response! id {:out (str "<" i ":" j ">")}))))
                  (range 8))]
    (deliver start true)
    (doseq [worker workers] (is (not= ::timeout (deref worker 5000 ::timeout))))
    (let [output (:output (api/nrepl-transcript id))]
      (is (= 160 (count (re-seq #"<\d+:\d+>" output))))
      (doseq [i (range 8) j (range 20)]
        (is (str/includes? output (str "<" i ":" j ">"))))
      (is (false? (:output-truncated? (api/nrepl-transcript id)))))))
