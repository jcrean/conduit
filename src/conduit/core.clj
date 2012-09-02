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

(defn pipeline [data-producer data-consumer & stream-filters]
  (let [producer-istream (PipedInputStream.)
        producer-ostream (apply-stream-filters
                          (PipedOutputStream. producer-istream)
                          stream-filters)
        producer-pipe    (pipe-writer producer-ostream data-producer)
        consumer         (pipe-reader producer-istream data-consumer)]
    (prn "awaiting producer to complete...")
    @producer-pipe
    (prn "producer finished writing to pipeline.")
    ))


(comment

  (defn send-data-into-pipe [^java.io.OutputStream ostream]
    (prn "sending msg into pipe")
    (let [msg "hello there"
          msg-bytes (.getBytes msg "UTF-8")]
      (.write ostream msg-bytes 0 (count msg-bytes))
      (prn "done writing")))


  (defn collect-data-from-pipe [^java.io.InputStream istream]
    (prn "collecting message from pipe")
    (let [bos (java.io.ByteArrayOutputStream.)]
      (IOUtils/copy istream bos)
      (prn (str "received message: " (.toString bos "UTF-8")))))

  (defn write-data-to-file [^java.io.InputStream istream]
    (let [fos (java.io.FileOutputStream. "/tmp/foo.gz")]
      (IOUtils/copy istream fos)))

  (pipeline send-data-into-pipe collect-data-from-pipe)

  (pipeline send-data-into-pipe write-data-to-file :gzip)

  )