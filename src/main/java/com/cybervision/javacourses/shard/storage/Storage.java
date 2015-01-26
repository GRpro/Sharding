package com.cybervision.javacourses.shard.storage;

import com.cybervision.javacourses.crud.CRUD;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 * Created by Grigoriy on 1/17/2015.
 */
public class Storage<K, V> implements CRUD<K, V> {

    private Logger logger;
    private String path;
    private File file;

    /**
     * Used for locking all CRUD methods when cash is loading or writing.
     * Also it prevents simultaneous invocation of methods {@code loadCash()} and {@code writeCash()}
     */
    private ReadWriteLock lock;

    private ConcurrentHashMap<K, V> cash;

    /**
     * Constructs storage manager
     * @param path special path to file including file name
     * @param logger logger object
     */
    public Storage(String path, Logger logger) {
        this.logger = logger;
        this.lock = new ReentrantReadWriteLock(true);
        configureStorage(path);

    }

    private void configureStorage(String path) {
        this.path = path;
        this.file = new File(path);
        if (!file.exists()) {
            try {
                file.getParentFile().mkdirs();
                if (file.createNewFile() == false) {
                    logger.error("storage \"" + file.getAbsoluteFile() + "\" cannot be created");
                    System.exit(-1);
                }
                logger.info("storage \"" + file.getAbsoluteFile() + "\" was created");
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(-1);
            }
            this.cash = new ConcurrentHashMap<>();
        } else {
            logger.info("storage \"" + file.getAbsoluteFile() + "\" already exists");
            loadCash();
        }
    }

    private void loadCash() {
        lock.writeLock().lock();

        try(ObjectInputStream objectInputStream = new ObjectInputStream(new BufferedInputStream(new FileInputStream(file)))) {
            Object o = objectInputStream.readObject();
            if (o == null) {
                this.cash = new ConcurrentHashMap<>();
            } else {
                this.cash = (ConcurrentHashMap<K, V>) o;
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void writeCash() {
        lock.writeLock().lock();
        try(ObjectOutputStream objectOutputStream = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(file)))) {
            objectOutputStream.writeObject(cash);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public boolean create(K key, V value) {
        lock.readLock().lock();
        boolean result = false;
        if (!cash.containsKey(key)) {
            cash.put(key, value);
            result = true;
        }
        lock.readLock().unlock();
        return result;
    }

    @Override
    public V read(K key) {
        lock.readLock().lock();
        V result = (V) cash.get(key);
        lock.readLock().unlock();
        return result;
    }

    @Override
    public boolean update(K key, V newValue) {
        lock.readLock().lock();
        boolean result = false;
        if (cash.containsKey(key)) {
            cash.replace(key, newValue);
            result = true;
        }
        lock.readLock().unlock();
        return result;
    }

    @Override
    public boolean delete(K key) {
        lock.readLock().lock();
        boolean result = false;
        if (cash.containsKey(key)) {
            cash.remove(key);
            result = true;
        }
        lock.readLock().unlock();
        return result;
    }

    public void endSession() {
        writeCash();
    }
}
