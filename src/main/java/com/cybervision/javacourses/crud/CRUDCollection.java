package com.cybervision.javacourses.crud;

import java.util.Collection;

public interface CRUDCollection<K, V> {

    public boolean createCollection(K key, Collection<V> collection);
    public void readCollection(K key, Collection<V> collection, Class<V> cl);
    public boolean updateCollection(K key, Collection<V> newCollection);
    public boolean deleteCollection(K key);
}
