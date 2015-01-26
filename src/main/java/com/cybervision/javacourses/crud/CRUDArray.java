package com.cybervision.javacourses.crud;

public interface CRUDArray<K, V> {

    public boolean createArray(K key, V[] array);
    public V[] readArray(K key, Class<V> cl);
    public boolean updateArray(K key, V[] array);
    public boolean deleteArray(K key);
}
