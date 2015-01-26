package com.cybervision.javacourses.crud;

/**
 * Define common set of operations called CRUD, which every storage should have:
 * create, read, update, delete
 * @param <K> Type of special keys
 */
public interface CRUD<K, V> {

    public boolean create(K key, V value);

    public V read(K key);

    public boolean update(K key, V newValue);

    public boolean delete(K key);

}
