(ns conduit.core
  (:require
   [rn.clorine.pool     :as pool])
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
  (Thread.
   (fn []
     (func pipe)
     (close-pipe pipe))))

(defn pipe-reader [^PipedInputStream pipe func]
  (Thread.
   (fn []
     (func pipe))))

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
    (.start producer)
    (.start consumer)
    {:producer producer
     :consumer consumer}))

(defn basic-conduit [ostream-producer istream-consumer]
  (pipeline ostream-producer istream-consumer))

(defn gzip-conduit [ostream-producer istream-consumer]
  (pipeline ostream-producer istream-consumer :gzip))


(defprotocol Conduit
  (pipe-data [this ostream-producer-fn istream-consumer-fn]))

(defn create-conduit [& args]
  (reify
   Conduit
   (pipe-data
    [this ostream-producer-fn istream-consumer-fn]
    (pipeline ostream-producer-fn istream-consumer-fn))))


(defn initialize []
  (pool/register-pool
   :core-conduits
   (pool/make-factory {:make-fn create-conduit})))

(defn conduit! [name data-producer data-consumer & stream-filters]
  (pool/with-instance [conduit :core-conduits]
    (pipe-data conduit data-producer data-consumer)))


(initialize)