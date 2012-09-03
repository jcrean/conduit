(ns conduit.core
  (:import
   [org.apache.commons.io IOUtils]
   [java.io PipedInputStream PipedOutputStream ByteArrayInputStream]))

(defn close-pipe [pipe-ostream]
  (try
   (.flush pipe-ostream)
   (.close pipe-ostream)
   (catch Exception ex
     ;; What to do here? log?
     )))

(defn pipe-writer [^PipedOuputStream pipe func]
  (future
    (func pipe)
    (close-pipe pipe)))

(defn pipe-reader [^PipedInputStream pipe func]
  (future
    (func pipe)))

(def *stream-filter-wrappers*
     {:gzip (fn [ostream] (java.util.zip.GZIPOutputStream. ostream))})

(defn apply-stream-filters [ostream filters]
  (reduce (fn [stream filter]
            (if-let [wrapper-fn (get *stream-filter-wrappers* filter)]
              (wrapper-fn stream)
              ;; NB: maybe just raise here?
              stream))
          ostream
          filters))

(defn pipeline [ostream-producer-fn istream-consumer-fn & stream-filters]
  (let [producer-istream (PipedInputStream.)
        producer-ostream (apply-stream-filters
                          (PipedOutputStream. producer-istream)
                          stream-filters)
        producer         (pipe-writer producer-ostream ostream-producer-fn)
        consumer         (pipe-reader producer-istream istream-consumer-fn)]
    {:producer producer
     :consumer consumer}))

(defn basic-conduit [ostream-producer istream-consumer]
  (pipeline ostream-producer istream-consumer))

(defn gzip-conduit [ostream-producer istream-consumer]
  (pipeline ostream-producer istream-consumer :gzip))
