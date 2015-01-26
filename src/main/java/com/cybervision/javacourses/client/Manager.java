package com.cybervision.javacourses.client;

import com.cybervision.javacourses.communication.ConnectionData;
import com.cybervision.javacourses.communication.Request;

import java.io.*;

import com.cybervision.javacourses.communication.Response;
import com.cybervision.javacourses.communication.meta.MetaRequest;
import com.cybervision.javacourses.communication.meta.MetaResponse;
import com.cybervision.javacourses.crud.CRUDArray;
import com.cybervision.javacourses.crud.CRUDCollection;
import com.cybervision.javacourses.multithreading.SupplierManager;
import com.cybervision.javacourses.crud.CRUD;
import com.cybervision.javacourses.multithreading.ConsumerManager;

import java.lang.reflect.Array;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created by Grigoriy on 1/3/2015.
 */
public class Manager<K extends Serializable, V extends Serializable> implements
        CRUD<K, V>, CRUDCollection<K, V>, CRUDArray<K, V> {

    private String host;
    private int port;

    /**Needs just to store all used addresses during session*/
    private Set<Address> usedAddresses;

    private class Address {
        private String host;
        private int port;

        public Address(String host, int port) {
            this.host = host;
            this.port = port;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Address address = (Address) o;

            if (port != address.port) return false;
            if (!host.equals(address.host)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = host.hashCode();
            result = 31 * result + port;
            return result;
        }

        public String getHost() {return host;}
        public int getPort() {return port;}
    }

    private class Saver implements Callable<Boolean>{
        private String host;
        private int port;

        public Saver(String host, int port) {
            this.host = host;
            this.port = port;
        }

        @Override
        public Boolean call() throws Exception {
            try (Socket s = new Socket(host, port);
                 ObjectInputStream objectInputStream = new ObjectInputStream(s.getInputStream());
                 ObjectOutputStream objectOutputStream = new ObjectOutputStream(s.getOutputStream())) {
                Request r = new Request(Request.Operation.SAVE_SESSION, null, null, null);
                objectOutputStream.writeObject(r);
                return (boolean) ((Response) objectInputStream.readObject()).getData();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            return false;
        }
    }

    /**
     * @param host special host on which Master is running
     * @param port special port on which Master is running
     */
    public Manager(String host, int port) {
        this.host = host;
        this.port = port;
        this.usedAddresses = new HashSet<>();
    }

    private synchronized Socket getSocket(K key) {
        try (Socket metaSocket = new Socket(host, port);
             ObjectOutputStream metaOut = new ObjectOutputStream(metaSocket.getOutputStream());
             ObjectInputStream metaIn = new ObjectInputStream(metaSocket.getInputStream())) {

            MetaRequest metaRequest = new MetaRequest(key.hashCode());
            metaOut.writeObject(metaRequest);
            MetaResponse metaResponse = (MetaResponse) metaIn.readObject();

            //Store address
            usedAddresses.add(new Address(metaResponse.getHost(), metaResponse.getPort()));

            return new Socket(metaResponse.getHost(), metaResponse.getPort());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void saveSession() {
        if (usedAddresses.size() > 0) {
            ExecutorService exec = Executors.newFixedThreadPool(usedAddresses.size());
            FutureTask<Saver>[] tasks = new FutureTask[usedAddresses.size()];
            int count = 0;
            for (Address usedAddress : usedAddresses) {
                tasks[count] = new FutureTask<>(new Manager.Saver(usedAddress.getHost(), usedAddress.getPort()));
                exec.submit(tasks[count++]);
            }
            exec.shutdown();
            for (int i = 0; i < tasks.length; i++) {
                try {
                    tasks[i].get();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private Socket[] constructSockets(ConnectionData connectionData) {
        String host = connectionData.getHost();
        int[] ports = connectionData.getPorts();
        Socket[] sockets = new Socket[ports.length];
        for (int i = 0; i < sockets.length; i++) {
            try {
                sockets[i] = new Socket(host, ports[i]);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return sockets;
    }

    //*********************************************CRUD operations***********************************************//


    private boolean createSafety(K key, Object[] array, Request.Element element) {
        try (Socket s = getSocket(key);
             ObjectInputStream objectInputStream = new ObjectInputStream(s.getInputStream());
             ObjectOutputStream objectOutputStream = new ObjectOutputStream(s.getOutputStream())) {
            Request r = new Request(Request.Operation.CREATE, element, array.length, key);
            objectOutputStream.writeObject(r);

            ConnectionData connectionData = (ConnectionData) objectInputStream.readObject();
            new SupplierManager(array, constructSockets(connectionData));

            return (boolean) ((Response) objectInputStream.readObject()).getData();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return false;
    }

    private V[] readSafety(K key, Request.Element element, Class<V> cl) {
        try (Socket s = getSocket(key);
             ObjectOutputStream objectOutputStream = new ObjectOutputStream(s.getOutputStream());
             ObjectInputStream objectInputStream = new ObjectInputStream(s.getInputStream())) {

            Request r = new Request(Request.Operation.READ, element, null, key);
            objectOutputStream.writeObject(r);
            ConnectionData connectionData = (ConnectionData) objectInputStream.readObject();
            if (connectionData == null) {
                return null;
            }
            ConsumerManager consumer = new ConsumerManager(constructSockets(connectionData), (Integer) connectionData.getOther());
            V[] elements = (V[]) consumer.getArray(cl);
            return elements;
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    private boolean updateSafety(K key, Object[] array,  Request.Element element) {
        try (Socket s = getSocket(key);
             ObjectInputStream objectInputStream = new ObjectInputStream(s.getInputStream());
             ObjectOutputStream objectOutputStream = new ObjectOutputStream(s.getOutputStream())) {


            Request r = new Request(Request.Operation.UPDATE, element, array.length, key);
            objectOutputStream.writeObject(r);

            ConnectionData connectionData = (ConnectionData) objectInputStream.readObject();
            new SupplierManager(array, constructSockets(connectionData));

            return (boolean) ((Response) objectInputStream.readObject()).getData();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return false;
    }

    private boolean deleteSafety(K key, Request.Element element) {
        try (Socket s = getSocket(key);
             ObjectInputStream objectInputStream = new ObjectInputStream(s.getInputStream());
             ObjectOutputStream objectOutputStream = new ObjectOutputStream(s.getOutputStream())) {

            Request r = new Request(Request.Operation.DELETE, element, null, key);
            objectOutputStream.writeObject(r);
            return (boolean) ((Response) objectInputStream.readObject()).getData();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return false;
    }

    //***********************************************Public Methods**************************************************//


    @Override
    public boolean create(K key, V value) {
        try (Socket s = getSocket(key);
             ObjectInputStream objectInputStream = new ObjectInputStream(s.getInputStream());
             ObjectOutputStream objectOutputStream = new ObjectOutputStream(s.getOutputStream())) {

            Request r = new Request(Request.Operation.CREATE, Request.Element.SINGLE_OBJECT, value, key);
            objectOutputStream.writeObject(r);
            return (boolean) ((Response) objectInputStream.readObject()).getData();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public V read(K key) {
        try (Socket s = getSocket(key);
             ObjectInputStream objectInputStream = new ObjectInputStream(s.getInputStream());
             ObjectOutputStream objectOutputStream = new ObjectOutputStream(s.getOutputStream())) {

            Request r = new Request(Request.Operation.READ, Request.Element.SINGLE_OBJECT, null, key);
            objectOutputStream.writeObject(r);
            return (V) ((Response) objectInputStream.readObject()).getData();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public boolean update(K key, V newValue) {
        try (Socket s = getSocket(key);
             ObjectInputStream objectInputStream = new ObjectInputStream(s.getInputStream());
             ObjectOutputStream objectOutputStream = new ObjectOutputStream(s.getOutputStream())) {

            Request r = new Request(Request.Operation.UPDATE, Request.Element.SINGLE_OBJECT, newValue, key);
            objectOutputStream.writeObject(r);
            return (boolean) ((Response) objectInputStream.readObject()).getData();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public boolean delete(K key) {
        return deleteSafety(key, Request.Element.SINGLE_OBJECT);
    }

    @Override
    public boolean createCollection(K key, Collection<V> collection) {
        Object[] elements = collection.toArray();
        return createSafety(key, elements, Request.Element.COLLECTION);
    }

    @Override
    public void readCollection(K key, Collection<V> collection, Class<V> cl) {
        if (collection == null)
            throw new IllegalArgumentException("null collection");
        Object[] elements = readSafety(key, Request.Element.COLLECTION, cl);
        if (elements != null) {
            for (int i = 0; i < elements.length; i++) {
                collection.add((V) elements[i]);
            }
        } else {
            throw new NullPointerException("there is no collection with key " + key);
        }
    }

    @Override
    public boolean updateCollection(K key, Collection<V> newCollection) {
        Object[] elements = newCollection.toArray();
        return updateSafety(key, elements, Request.Element.COLLECTION);
    }

    @Override
    public boolean deleteCollection(K key) {
        return deleteSafety(key, Request.Element.COLLECTION);
    }

    @Override
    public boolean createArray(K key, V[] array) {
        return createSafety(key, array, Request.Element.ARRAY);
    }

    @Override
    public V[] readArray(K key, Class<V> cl) {
        Object[] objects = readSafety(key, Request.Element.ARRAY, cl);
        if (objects == null)
            return null;
        V[] array = (V[]) Array.newInstance(cl, objects.length);
        for (int i = 0; i < objects.length; i++) {
            array[i] = (V) objects[i];
        }
        return array;
    }

    @Override
    public boolean updateArray(K key, V[] array) {
        return updateSafety(key, array, Request.Element.ARRAY);
    }

    @Override
    public boolean deleteArray(K key) {
        return deleteSafety(key, Request.Element.ARRAY);
    }




}
