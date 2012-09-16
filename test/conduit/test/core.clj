(ns conduit.test.core
  (:use [conduit.core])
  (:use [clojure.test]))

(defn make-basic-ostream-producer [msg]
  (let [msg-bytes (.getBytes msg "UTF-8")]
    (fn [^java.io.OutputStream ostream]
      (.write ostream msg-bytes 0 (count msg-bytes)))))

(defn make-byte-array-consumer [res]
  (fn [^java.io.InputStream istream]
    (let [bos (java.io.ByteArrayOutputStream.)]
      (org.apache.commons.io.IOUtils/copy istream bos)
      (reset! res (.toByteArray bos)))))

(deftest test-basic-conduit
  (let [msg         "this is a test!"
        collector   (atom (byte-array 0))
        producer-fn (make-basic-ostream-producer msg)
        consumer-fn (make-byte-array-consumer collector)
        conduit     (conduit! :basic producer-fn consumer-fn)]
    (.join (:consumer conduit))
    (is (= msg (String. @collector "UTF-8")))))

#_(deftest test-gzip-conduit
  (let [msg         "this is a test of gzipping!"
        collector   (atom (byte-array 0))
        producer-fn (make-basic-ostream-producer msg)
        consumer-fn (make-byte-array-consumer collector)
        conduit     (gzip-conduit producer-fn consumer-fn)
        unzipper    (fn [bytes]
                      (let [istream (java.util.zip.GZIPInputStream.
                                     (java.io.ByteArrayInputStream. bytes))]
                        (org.apache.commons.io.IOUtils/toByteArray istream)))]
    @(:consumer conduit)
    (is (= msg (String. (unzipper @collector) "UTF-8")))))


(comment
  (let [msg         "this is a test!"
        collector   (atom (byte-array 0))
        producer-fn (make-basic-ostream-producer msg)
        consumer-fn (make-byte-array-consumer collector)
        conduit     (conduit! :basic producer-fn consumer-fn)]
    (.join (:consumer conduit)))
)