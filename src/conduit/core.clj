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

(defn pipe []
  (PipedOutputStream. (PipedInputStream.)))

(defn pipe-writer [^PipedOuputStream pipe func]
  (Thread.
   (fn []
     (func pipe)
     (close-pipe pipe))))

(defn pipe-reader [^PipedInputStream pipe func]
  (Thread.
   (fn []
     (func pipe))))

(comment

  (defn send-data-into-pipe [^java.io.OutputStream ostream]
    (let [msg "hello there"
          msg-bytes (.getBytes msg "UTF-8")]
      (.write ostream msg-bytes 0 (count msg-bytes))))


  (defn collect-data-from-pipe [^java.io.InputStream istream]
    (let [bos (java.io.ByteArrayOutputStream.)]
      (IOUtils/copy istream bos)
      (prn (str "received message: " (.toString bos "UTF-8")))))


  (defn pipeline [pipe-writer-fn & sinks]
    (let [pis1       (PipedInputStream.)
          pos1       (PipedOutputStream. pis1)
          pipe-start (pipe-writer pos1 pipe-writer-fn)
          pipe-end   (pipe-reader pis1 (first sinks))]
      (.start pipe-start)
      (.start pipe-end)
      (.join pipe-end)))

  (pipeline
   send-data-into-pipe
   collect-data-from-pipe
   )

  )