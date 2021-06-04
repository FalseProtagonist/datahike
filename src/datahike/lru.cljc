(ns ^:no-doc datahike.lru
  (:require [clojure.core.cache :refer [defcache CacheProtocol]]
            clojure.data.priority-map))

(declare assoc-lru cleanup-lru)

#?(:cljs
   (deftype LRU [key-value gen-key key-gen gen limit]
     IAssociative
     (-assoc [this k v] (assoc-lru this k v))
     (-contains-key? [_ k] (-contains-key? key-value k))
     ILookup
     (-lookup [_ k]    (-lookup key-value k nil))
     (-lookup [_ k nf] (-lookup key-value k nf))
     IPrintWithWriter
     (-pr-writer [_ writer opts]
       (-pr-writer (persistent! key-value) writer opts)))
   :clj
   (deftype LRU [^clojure.lang.Associative key-value gen-key key-gen gen limit]
     clojure.lang.ILookup
     (valAt [_ k]           (.valAt key-value k))
     (valAt [_ k not-found] (.valAt key-value k not-found))
     clojure.lang.Associative
     (containsKey [_ k] (.containsKey key-value k))
     (entryAt [_ k]     (.entryAt key-value k))
     (assoc [this k v]  (assoc-lru this k v))))

(defn assoc-lru [^LRU lru k v]
  (let [key-value (.-key-value lru)
        gen-key   (.-gen-key lru)
        key-gen   (.-key-gen lru)
        gen       (.-gen lru)
        limit     (.-limit lru)]
    (if-let [g (key-gen k nil)]
      (->LRU key-value
             (-> gen-key
                 (dissoc g)
                 (assoc gen k))
             (assoc key-gen k gen)
             (inc gen)
             limit)
      (cleanup-lru
       (->LRU (assoc key-value k v)
              (assoc gen-key gen k)
              (assoc key-gen k gen)
              (inc gen)
              limit)))))

(defn cleanup-lru [^LRU lru]
  (if (> (count (.-key-value lru)) (.-limit lru))
    (let [key-value (.-key-value lru)
          gen-key   (.-gen-key lru)
          key-gen   (.-key-gen lru)
          gen       (.-gen lru)
          limit     (.-limit lru)
          [g k]     (first gen-key)]
      (->LRU (dissoc key-value k)
             (dissoc gen-key g)
             (dissoc key-gen k)
             gen
             limit))
    lru))

(defn lru [limit]
  (->LRU {} (sorted-map) {} 0 limit))

(defcache LRUDatomCache [cache lru counts n-total-datoms tick datom-limit]
  CacheProtocol
  (lookup [_ item]
          (get cache item))
  (lookup [_ item not-found]
          (get cache item not-found))
  (has? [_ item]
        (contains? cache item))
  (hit [_ item]
       (let [tick+ (inc tick)]
         (LRUDatomCache. cache
                         (if (contains? cache item)
                           (assoc lru item tick+)
                           lru)
                         counts
                         n-total-datoms
                         tick+
                         datom-limit)))
  (miss [this item result]
        (let [tick+ (inc tick)
              n-new-datoms (count result)
              new-size (+ n-total-datoms n-new-datoms)
              [c l n s] (if (contains? lru item)
                          [(dissoc cache item)
                           (dissoc lru item)
                           (dissoc counts item)
                           (- new-size (get counts item))]
                          [cache lru counts new-size])
              [c l n s] (loop [c c l l n n s s]
                          (if (> s datom-limit)
                            (let [k (first (peek lru))]
                              (recur (dissoc c k)
                                     (dissoc l k)
                                     (dissoc n k)
                                     (- s (get n k))))
                            [c l n s]))]
          (LRUDatomCache. (assoc c item result)
                          (assoc l item tick+)
                          (assoc n item n-new-datoms)
                          s
                          tick+
                          datom-limit)))
  (evict [this key]
         (if (contains? cache key)
           (LRUDatomCache. (dissoc cache key)
                           (dissoc lru key)
                           (dissoc counts key)
                           (inc tick)
                           (- n-total-datoms (get counts key))
                           datom-limit)
           this))
  (seed [_ base]
        (LRUDatomCache. base
                        (into (clojure.data.priority-map/priority-map)
                              (concat (take (- datom-limit (count base)) (for [k (range (- datom-limit) 0)] [k k]))
                                      (for [[k _] base] [k 0])))
                        (zipmap (keys base)
                                (map #(count (get base key))
                                     (keys base)))
                        0
                        0
                        datom-limit))
  Object
  (toString [_]
            (str cache \, \space lru \, \space counts \, \space n-total-datoms \, \space tick \, \space datom-limit)))

(defn lru-datom-cache-factory
  "Returns an LRU cache with the cache and usage-table initialied to `base` --
   each entry is initialized with the same usage value.
   This function takes an optional `:threshold` argument that defines the maximum number
   of elements in the cache before the LRU semantics apply (default is 32)."
  [base & {threshold :threshold :or {threshold 32}}]
  {:pre [(number? threshold) (< 0 threshold)
         (map? base)]}
  (atom (clojure.core.cache/seed (LRUDatomCache. {} (clojure.data.priority-map/priority-map) {} 0 0 threshold) base)))
