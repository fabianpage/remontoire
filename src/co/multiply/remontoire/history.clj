(ns co.multiply.remontoire.history
  "Bounded, best-effort nREPL transcripts, independent of the task registry.
   No nREPL dependency here: only copied strings and fixed bookkeeping enter
   the store. The optional middleware owns protocol interception."
  (:import
    (java.util.concurrent.atomic AtomicLong)
    (java.util.concurrent.locks ReentrantLock)))


(def ^:private defaults
  {:enabled? false :max-entries 200 :entry-bytes 65536 :total-bytes 8388608})


(defonce ^:private state
  (atom {:config defaults :entries (sorted-map) :next-id 0 :bytes 0
         :evicted-through 0 :cleared-through 0 :pauses 0}))


(defonce ^:private writer (ReentrantLock.))
(defonce ^:private gaps (AtomicLong.))


(defn gap!
  "Record a missed capture without retaining the exception or message."
  []
  (AtomicLong/.incrementAndGet gaps)
  nil)


(defn- capture!
  "Never wait for another writer, retry a transaction, or propagate a capture
   failure into nREPL. Control operations may lock; the recording path may not."
  [f]
  (try
    (when (get-in @state [:config :enabled?])
      (if (ReentrantLock/.tryLock writer)
        (try
          (when (get-in @state [:config :enabled?]) (f))
          (finally (ReentrantLock/.unlock writer)))
        (gap!)))
    (catch Throwable _ (gap!))))


(defn- prefix
  "Copy a UTF-8-bounded prefix without encoding or scanning the entire input.
   Never split a surrogate pair. Return [text byte-count truncated?]."
  [^String s budget]
  (let [length (String/.length s)]
    (loop [i 0 used 0]
      (if (= i length)
        [(String. (String/.substring s 0 i)) used false]
        (let [cp (String/.codePointAt s i)
              width (Character/charCount cp)
              bytes (cond (<= cp 0x7f) 1 (<= cp 0x7ff) 2
                          (<= 0xd800 cp 0xdfff) 1
                          (<= cp 0xffff) 3 :else 4)]
          (if (> (+ used bytes) budget)
            [(String. (String/.substring s 0 i)) used true]
            (recur (+ i width) (long (+ used bytes)))))))))


(defn- text-field
  "Charge text and a fixed allowance per field. Unknown objects are omitted,
   never printed, dereferenced, or walked. Even empty strings cost space."
  [entry path value limit]
  (cond
    (nil? value) entry
    (not (string? value)) (assoc entry :incomplete? true)
    (< (- limit (:bytes entry)) 32) (assoc entry :truncated? true)
    :else
    (let [[s bytes truncated?] (prefix value (- limit (:bytes entry) 32))]
      (cond-> (-> entry
                  (assoc-in path s)
                  (update :bytes + bytes 32))
        truncated? (assoc :truncated? true)))))


(defn- evict
  [s]
  (let [{:keys [max-entries total-bytes]} (:config s)]
    (loop [s s]
      (if (or (> (count (:entries s)) max-entries) (> (:bytes s) total-bytes))
        (let [[id entry] (first (:entries s))]
          (recur (-> s
                     (update :entries dissoc id)
                     (update :bytes - (:bytes entry))
                     (assoc :evicted-through id))))
        s))))


(defn- put-entry
  [s entry]
  (let [old-bytes (get-in s [:entries (:id entry) :bytes] 0)]
    (evict (-> s
               (assoc-in [:entries (:id entry)] entry)
               (update :bytes + (- (:bytes entry) old-bytes))))))


(defn begin!
  "Capture an eval/load-file request. Returns an id, or nil when not recording.
   Only the middleware calls this; request maps are never retained."
  [{:keys [op session id ns file file-path file-name code line column]}]
  (capture!
    (fn []
      (let [s @state
            next-id (inc (:next-id s))
            limit (get-in s [:config :entry-bytes])
            entry {:id next-id :op op :at (System/currentTimeMillis)
                   :status :running :bytes 256 :events []
                   :gap-at (AtomicLong/.get gaps)}
            entry (reduce (fn [e [k v]] (text-field e [k] v limit)) entry
                    [[:session session] [:request-id id] [:ns ns]
                     [:file (if (= op "load-file") file-path file)]
                     [:file-name file-name]])
            entry (assoc entry :identity-complete?
                    (and (= session (:session entry)) (= id (:request-id entry))))
            entry (cond-> entry
                    (int? line) (assoc :line line)
                    (int? column) (assoc :column column))
            entry (text-field entry [:input] (if (= op "load-file") file code) limit)]
        (reset! state (put-entry (assoc s :next-id next-id) entry))
        next-id))))


(defn- status-values
  [status]
  (cond
    (nil? status) []
    (or (string? status) (keyword? status)) [status]
    (coll? status) (take 33 status)
    :else [status]))


(defn- statuses
  [status]
  ;; nREPL uses keywords before bencode and strings after it. Keep a bounded
  ;; set of known evidence; never intern arbitrary client-provided names.
  (let [known {"done" :done :done :done
               "interrupted" :interrupted :interrupted :interrupted
               "eval-error" :eval-error :eval-error :eval-error
               "error" :error :error :error
               "print-quota-exceeded" :print-quota-exceeded
               :print-quota-exceeded :print-quota-exceeded
               "nrepl.middleware.print/truncated" :nrepl.middleware.print/truncated
               :nrepl.middleware.print/truncated :nrepl.middleware.print/truncated}
        xs (take 32 (status-values status))]
    (into #{} (keep known) xs)))


(defn- update-outcome
  "Track completion even when the transcript has no room for more text."
  [entry response]
  (let [flags (statuses (:status response))
        outcome (cond
                  (or (= :interrupted (:outcome entry)) (flags :interrupted)) :interrupted
                  (or (= :failed (:outcome entry)) (flags :error) (flags :eval-error)
                      (:ex response)) :failed
                  :else :done)
        terminal? (or (flags :done) (flags :interrupted))]
    (cond-> (assoc entry :outcome outcome)
      terminal? (assoc :status outcome :finished-at (System/currentTimeMillis))
      (or (flags :print-quota-exceeded)
          (flags :nrepl.middleware.print/truncated)) (assoc :incomplete? true))))


(defn- append-event
  [entry response limit]
  (let [fields [:value :out :err :ex :root-ex :ns :nrepl.middleware.caught/throwable]
        meaningful? (or (some? (:status response)) (some #(contains? response %) fields))]
    (cond
      (not meaningful?) entry
      (< (- limit (:bytes entry)) 64) (assoc entry :truncated? true)
      :else
      (let [i (count (:events entry))
            values (vec (status-values (:status response)))
            entry (cond-> (-> entry
                              (update :bytes + 64)
                              (update :events conj {:at (System/currentTimeMillis) :status []}))
                    (> (count values) 32) (assoc :truncated? true))
            entry (reduce (fn [e v]
                            (let [v (if (keyword? v) (subs (str v) 1) v)
                                  j (count (get-in e [:events i :status]))]
                              (text-field e [:events i :status j] v limit)))
                    entry (take 32 values))]
        (reduce (fn [e k] (text-field e [:events i k] (get response k) limit))
          entry fields)))))


(defn- add-response
  [entry response limit]
  (let [entry (update-outcome entry response)
        ;; The response can resolve an ephemeral session's id. Bound its copy
        ;; just like every other field, and do not retain the session atom.
        entry (if (nil? (:session entry))
                (text-field entry [:session] (:session response) limit)
                entry)]
    (append-event entry response limit)))


(defn response!
  "Append an outgoing response to a retained request, if still recording.
   Late responses cannot resurrect an evicted or cleared entry."
  [id response]
  (capture!
    (fn []
      (when-let [entry (get-in @state [:entries id])]
        (reset! state (put-entry @state
                        (add-response entry response (get-in @state [:config :entry-bytes]))))))))


(defn interrupt!
  "Interrupt notifications use the interrupt request's transport, not the
   original eval's. Match the original request id AND session; never guess."
  [response]
  (when ((statuses (:status response)) :interrupted)
    (capture!
      (fn []
        (let [s @state
              entry (some (fn [[_ e]]
                            (when (and (:identity-complete? e)
                                       (string? (:session response))
                                       (string? (:id response))
                                       (= (:session e) (:session response))
                                       (= (:request-id e) (:id response)))
                              e))
                      (rseq (:entries s)))]
          (when entry
            (reset! state (put-entry s
                            (add-response entry response (get-in s [:config :entry-bytes]))))))))))


(defn- visible-entry
  [entry gap-count]
  (let [missed? (> gap-count (:gap-at entry))]
    (cond-> (dissoc entry :gap-at :identity-complete? :outcome)
      missed? (assoc :capture-gaps? true)
      (and missed? (= :running (:status entry))) (assoc :status :unknown))))


(defn- status-of
  [s]
  (merge (:config s)
    (select-keys s [:bytes :evicted-through :cleared-through :cleared-at
                    :pauses :last-paused-at :last-enabled-at])
    {:entries (count (:entries s)) :latest-id (:next-id s)
     :capture-gaps (AtomicLong/.get gaps)}))


(defn status
  "Recording configuration, usage and loss counters. Byte budgets account for
   UTF-8 text plus fixed entry/field/event allowances, not exact JVM heap size.
   Positive :capture-gaps means capture failed or a writer was busy; affected
   entries are conservatively marked even when the lost message was elsewhere."
  []
  (status-of @state))


(defn recent
  "Newest-first summaries, no inputs or outputs. Options: :limit (default 20,
   capped at 200), :session, :status, :since (inclusive epoch milliseconds),
   :before (exclusive history id for pagination). The envelope always includes
   retention and capture-loss information, including when no entries match."
  ([] (recent {}))
  ([{:keys [limit session status since before] :or {limit 20}}]
   (when-not (and (int? limit) (<= 0 limit)
                 (or (nil? since) (int? since))
                 (or (nil? before) (int? before)))
     (throw (ex-info "Invalid history query bounds" {})))
   (let [s @state
         gap-count (AtomicLong/.get gaps)]
     {:capture (status-of s)
      :entries (->> (rseq (:entries s))
                    (map (fn [[_ e]] (visible-entry e gap-count)))
                    (filter #(and (or (nil? session) (= session (:session %)))
                                  (or (nil? status) (= status (:status %)))
                                  (or (nil? since) (<= since (:at %)))
                                  (or (nil? before) (< (:id %) before))))
                    (take (min 200 limit))
                    (mapv #(dissoc % :input :events)))})))


(defn transcript
  "One transcript, or an explicit :missing reason. History ids are not registry
   ids. Reading neither refreshes retention nor claims ownership of the result."
  [id]
  (when-not (and (int? id) (pos? id))
    (throw (ex-info "History id must be a positive integer" {})))
  (let [s @state]
    (if-let [entry (get-in s [:entries id])]
      (visible-entry entry (AtomicLong/.get gaps))
      {:id id :missing (cond
                         (<= id (:cleared-through s)) :cleared
                         (<= id (:next-id s)) :evicted
                         :else :unknown)})))


(defn- checked-config
  [config opts]
  (let [config (merge config opts)]
    (when-not (and (boolean? (:enabled? config))
                  (every? #(and (int? %) (pos? %))
                    (map config [:max-entries :entry-bytes :total-bytes]))
                  (<= 256 (:entry-bytes config)) (<= 256 (:total-bytes config)))
      (throw (ex-info "Invalid history limits" {})))
    config))


(defn- apply-capture-mode
  "Install the config, recording transitions and marking paused work incomplete."
  [s config now]
  (let [enabled? (:enabled? config)
        changed? (not= enabled? (get-in s [:config :enabled?]))
        s (cond-> (assoc s :config config)
            (and changed? enabled?) (assoc :last-enabled-at now)
            (and changed? (not enabled?))
            (assoc :last-paused-at now :pauses (inc (:pauses s 0))))]
    (if enabled?
      s
      (update s :entries
        (fn [entries]
          (reduce-kv (fn [entries id entry]
                       (if (= :running (:status entry))
                         (assoc entries id (assoc entry :status :unknown :incomplete? true))
                         entries))
            entries entries))))))


(defn- enforce-limits
  "Evict through the newest oversized entry, then enforce count and total bytes."
  [s]
  (let [limit (get-in s [:config :entry-bytes])
        oversized (->> (:entries s)
                       (keep (fn [[id e]] (when (> (:bytes e) limit) id)))
                       last)
        doomed (when oversized (take-while (fn [[id _]] (<= id oversized)) (:entries s)))]
    (evict
      (reduce (fn [s [id e]]
                (-> s
                    (update :entries dissoc id)
                    (update :bytes - (:bytes e))
                    (assoc :evicted-through id)))
        s doomed))))


(defn configure!
  "Merge capture options. Disabled until explicitly enabled. Limits are positive
   integers, with at least 256 bytes per entry and total. Reducing entry-bytes
   evicts entries already larger than it (oldest first through the last one).
   Pausing marks in-flight entries unknown/incomplete; resume cannot recover
   responses missed while paused. Control operations serialize with recording."
  [opts]
  (when-not (and (map? opts) (every? (set (keys defaults)) (keys opts)))
    (throw (ex-info "Unknown history configuration option" {})))
  (ReentrantLock/.lock writer)
  (try
    (let [s @state
          config (checked-config (:config s) opts)]
      (reset! state (-> s
                       (apply-capture-mode config (System/currentTimeMillis))
                       enforce-limits))
      (status))
    (finally (ReentrantLock/.unlock writer))))


(defn clear!
  "Delete retained content without changing capture settings or reusing ids.
   Already-fetched copies cannot be retracted. Loss counters remain cumulative."
  []
  (ReentrantLock/.lock writer)
  (try
    (swap! state #(assoc % :entries (sorted-map) :bytes 0
                   :cleared-through (:next-id %) :cleared-at (System/currentTimeMillis)))
    (status)
    (finally (ReentrantLock/.unlock writer))))
