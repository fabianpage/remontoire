(ns co.multiply.remontoire.nrepl-diagnostic-test
  (:require [clojure.test :as t]
            [co.multiply.remontoire.history :as history]
            [co.multiply.remontoire.nrepl-test :as nt]))

(t/deftest repeat-interrupt-attribution
  (let [gap! history/gap!
        evidence (atom [])]
    (with-redefs [history/gap!
                  (fn []
                    (swap! evidence conj
                      {:thread (Thread/.getName (Thread/currentThread))
                       :stack (mapv str (take 12 (Thread/.getStackTrace (Thread/currentThread))))
                       :entries (mapv #(select-keys % [:request-id :status :events])
                                  (vals (:entries @@#'history/state)))})
                    (gap!))]
      (dotimes [i 300]
        (reset! evidence [])
        (t/test-vars [#'nt/running-output-and-interrupts-are-attributed-to-the-original-eval])
        (when (seq @evidence)
          (println "[DEBUG-interrupt]" i @evidence))))))
