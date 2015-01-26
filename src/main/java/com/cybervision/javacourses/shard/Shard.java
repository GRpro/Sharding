package com.cybervision.javacourses.shard;

import com.cybervision.javacourses.communication.Response;
import com.cybervision.javacourses.multithreading.SupplierManager;
import com.cybervision.javacourses.communication.ConnectionData;
import com.cybervision.javacourses.communication.Request;
import com.cybervision.javacourses.multithreading.ConsumerManager;
import com.cybervision.javacourses.shard.storage.Storage;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Multithreaded Shard
 */
public class Shard implements Runnable {

    /** localhost */
    private String localhost;

    private int port;
    private Thread thread;
    private ServerSocket serverSocket;

    private boolean isAlive;

    private Storage<Object, Object> objectStorage;
    private Storage<Object, Object[]> arrayStorage;
    private Storage<Object, Object[]> collectionStorage;

    /**
     * Used to shelter right way of multithreaded connection
     * While all sockets in multithreaded operation not be connected
     * the thread from another request will be waiting while
     * all ServerSockets not be accepted their connection.
     * This is very significant thing. It allows use shard with
     * multithreading operations "Multithreadly".
     */
    private Lock mtLock;

    /**Special list of ServerSockets used for multithreaded operations*/
    private List<ServerSocket> serverSocketList;

    private Logger logger;

    private ExecutorService executorService;

    /**
     * Constructor
     * @param port
     */
    public Shard(int port) {
        //setup logging
        setupLogging();

        //setup other
        this.port = port;
        this.thread = new Thread(this, "Shard");
        this.executorService = Executors.newCachedThreadPool();

        this.serverSocketList = getAvailableServerSockets();

        try {
            this.localhost = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        //configuring storages
        this.objectStorage = new Storage<>("storage" + String.valueOf(port) +
                "\\" + "object_storage" + String.valueOf(port) + ".txt", logger);
        this.arrayStorage = new Storage<>("storage" + String.valueOf(port) +
                "\\" + "array_storage" + String.valueOf(port) + ".txt", logger);
        this.collectionStorage = new Storage<>("storage" + String.valueOf(port) +
                "\\" + "collection_storage" + String.valueOf(port) + ".txt", logger);

        this.mtLock = new ReentrantLock(true);
        try {
            serverSocket = new ServerSocket(port);
            logger.info("shard is going to be run on port " + port);
            logger.info("localhost: " + localhost);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private List<ServerSocket> getAvailableServerSockets() {
        List<ServerSocket> serverSocketList = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            try {
                ServerSocket ss = new ServerSocket(0);
                serverSocketList.add(ss);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return serverSocketList;
    }


    private void setupLogging() {
        this.logger = Logger.getLogger(Shard.class.getName());
        InputStream in = this.getClass().getClassLoader().getResourceAsStream("config/log4j.properties");
        Properties properties = new Properties();
        try {
            properties.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
        PropertyConfigurator.configure(properties);
    }


    @Override
    public void run() {
        logger.trace("shard is running");
        while(isAlive) {
            try {
                Socket socket = serverSocket.accept();
                executorService.submit(new Handler(socket));
            } catch (IOException e) {
//                e.printStackTrace();
            }
        }
    }


    public void start() {
        isAlive = true;
        thread.start();
    }

    public void stop() {
        isAlive = false;
        try {
            executorService.shutdown();
            boolean term = executorService.awaitTermination(15, TimeUnit.MINUTES);
            if (term) {
                logger.info("all threads have ended");
            } else {
                logger.error("not all threads have ended");
            }
            for (ServerSocket ss : serverSocketList) {
                ss.close();
            }
            objectStorage.endSession();
            arrayStorage.endSession();
            collectionStorage.endSession();
            if (!serverSocket.isClosed())
                serverSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        thread.interrupt();
        logger.info("shard was stopped");
    }


    //***********************************************Multithreading**************************************************//

    private int getThreadNumber(int arrayLength) {
        int result = arrayLength / 1000;
        if (result == 0)
            result = 1;
        if (result > serverSocketList.size())
            result = serverSocketList.size();
        return result;
    }

    private synchronized MCD acceptAll(int threadNumber) {
        int[] ports = new int[threadNumber];
        FutureTask<Socket>[] futureTasks = new FutureTask[threadNumber];
        ExecutorService executorService  = Executors.newFixedThreadPool(threadNumber);

        /*locking*/
        mtLock.lock();

        for (int i = 0; i < threadNumber; i++) {
            ports[i] = serverSocketList.get(i).getLocalPort();
            futureTasks[i] = (FutureTask<Socket>) executorService.submit(new Acceptor(serverSocketList.get(i)));
        }
        executorService.shutdown();
        return new MCD(ports, futureTasks);
    }

    private Socket[] retrieveSockets(FutureTask<Socket>[] futureTasks) {
        Socket[] sockets = new Socket[futureTasks.length];
        for (int i = 0; i < futureTasks.length; i++) {
            try {
                sockets[i] = futureTasks[i].get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }

        /*unlocking*/
        mtLock.unlock();

        return sockets;
    }

    //***********************************************Inner classes**************************************************//

    /**
     * Meta Connection Data
     */
    private class MCD {
        private int[] ports;
        private FutureTask<Socket>[] futureTasks;

        public MCD(int[] ports, FutureTask<Socket>[] futureTasks) {
            this.ports = ports;
            this.futureTasks = futureTasks;
        }
        public int[] getPorts() {return ports;}
        public FutureTask<Socket>[] getFutureTasks() {return futureTasks;}
    }

    private class Acceptor implements Callable<Socket> {
        private ServerSocket serverSocket;
        public Acceptor(ServerSocket serverSocket) {
            this.serverSocket = serverSocket;
        }
        @Override
        public Socket call() throws Exception {
            Socket socket = serverSocket.accept();
            return socket;
        }
    }

    /**
     * Encapsulates servicing logic for every request
     */
    public class Handler implements Callable<Void> {

        private Socket socket;

        public Handler(Socket socket) {
            this.socket = socket;
        }

        @Override
        public Void call() {
            try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                 ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream())) {

                Request r = (Request) objectInputStream.readObject();
                logger.trace("request was successfully got");
                switch (r.getOperation()) {
                    case CREATE: {
                        switch (r.getElement()) {
                            case SINGLE_OBJECT: {
                                boolean result = objectStorage.create(r.getKey(), r.getData());
                                if (result) {
                                    logger.debug("object with key " + r.getKey() + " was created");
                                } else {
                                    logger.debug("object with key " + r.getKey() + " wasn't created");
                                }
                                objectOutputStream.writeObject(new Response(Boolean.valueOf(result)));
                                break;
                            }
                            case ARRAY: {
                                int arrayLength = (int) r.getData();
                                //starting threads
                                MCD mcd = acceptAll(getThreadNumber((Integer) r.getData()));
                                objectOutputStream.writeObject(new ConnectionData(localhost, mcd.getPorts(), null));
                                //retrieving sockets
                                ConsumerManager consumer = new ConsumerManager(retrieveSockets(mcd.getFutureTasks()), arrayLength);
                                Object[] elements = consumer.getArray(Object.class);
                                boolean result = arrayStorage.create(r.getKey(), elements);
                                if (result) {
                                    logger.debug("array with key " + r.getKey() + " was created");
                                } else {
                                    logger.debug("array with key " + r.getKey() + " wasn't created");
                                }
                                objectOutputStream.writeObject(new Response(Boolean.valueOf(result)));
                                break;
                            }
                            case COLLECTION: {
                                int arrayLength = (int) r.getData();
                                //starting threads
                                MCD mcd = acceptAll(getThreadNumber((Integer) r.getData()));
                                objectOutputStream.writeObject(new ConnectionData(localhost, mcd.getPorts(), null));
                                //retrieving sockets
                                ConsumerManager consumer = new ConsumerManager(retrieveSockets(mcd.getFutureTasks()), arrayLength);
                                Object[] elements = consumer.getArray(Object.class);
                                boolean result = collectionStorage.create(r.getKey(), elements);
                                if (result) {
                                    logger.debug("collection with key " + r.getKey() + " was created");
                                } else {
                                    logger.debug("collection with key " + r.getKey() + " wasn't created");
                                }
                                objectOutputStream.writeObject(new Response(Boolean.valueOf(result)));
                                break;
                            }
                        }
                        break;
                    }
                    case READ: {
                        switch (r.getElement()) {
                            case SINGLE_OBJECT: {
                                Object result = objectStorage.read(r.getKey());
                                if (result != null) {
                                    logger.debug("object with key " + r.getKey() + " was read");
                                } else {
                                    logger.debug("object with key " + r.getKey() + " wasn't read");
                                }
                                objectOutputStream.writeObject(new Response(result));
                                break;
                            }
                            case ARRAY: {
                                Object[] elements = arrayStorage.read(r.getKey());
                                if (elements!= null) {
                                    logger.debug("array with key " + r.getKey() + " was read");
                                    //starting threads
                                    MCD mcd = acceptAll(getThreadNumber(elements.length));
                                    objectOutputStream.writeObject(new ConnectionData(localhost, mcd.getPorts(), elements.length));
                                    //retrieving sockets
                                    new SupplierManager(elements, retrieveSockets(mcd.getFutureTasks()));
                                } else {
                                    logger.debug("array with key " + r.getKey() + " wasn't read");
                                    objectOutputStream.writeObject(null);
                                }
                                break;
                            }
                            case COLLECTION: {
                                Object[] elements = collectionStorage.read(r.getKey());
                                if (elements!= null) {
                                    logger.debug("array with key " + r.getKey() + " was read");
                                    int arrayLength = elements.length;
                                    //starting threads

                                    MCD mcd = acceptAll(getThreadNumber(elements.length));
                                    objectOutputStream.writeObject(new ConnectionData(localhost, mcd.getPorts(), arrayLength));
                                    //retrieving sockets
                                    new SupplierManager(elements, retrieveSockets(mcd.getFutureTasks()));
                                } else {
                                    logger.debug("collection with key " + r.getKey() + " wasn't read");
                                    objectOutputStream.writeObject(null);
                                }
                                break;
                            }
                        }
                        break;
                    }
                    case UPDATE: {
                        switch (r.getElement()) {
                            case SINGLE_OBJECT: {
                                boolean result = objectStorage.update(r.getKey(), r.getData());
                                if (result) {
                                    logger.debug("object with key " + r.getKey() + " was updated");
                                } else {
                                    logger.debug("object with key " + r.getKey() + " wasn't updated");
                                }
                                objectOutputStream.writeObject(new Response(Boolean.valueOf(result)));
                                break;
                            }
                            case ARRAY: {
                                int arrayLength = (int) r.getData();
                                //starting threads
                                MCD mcd = acceptAll(getThreadNumber((Integer) r.getData()));
                                objectOutputStream.writeObject(new ConnectionData(localhost, mcd.getPorts(), null));
                                //retrieving sockets
                                ConsumerManager consumer = new ConsumerManager(retrieveSockets(mcd.getFutureTasks()), arrayLength);
                                Object[] elements = consumer.getArray(Object.class);
                                boolean result = arrayStorage.update(r.getKey(), elements);
                                if (result) {
                                    logger.debug("array with key " + r.getKey() + " was updated");
                                } else {
                                    logger.debug("array with key " + r.getKey() + " wasn't updated");
                                }
                                objectOutputStream.writeObject(new Response(Boolean.valueOf(result)));
                                break;
                            }
                            case COLLECTION: {
                                int arrayLength = (int) r.getData();
                                //starting threads
                                MCD mcd = acceptAll(getThreadNumber((Integer) r.getData()));
                                objectOutputStream.writeObject(new ConnectionData(localhost, mcd.getPorts(), null));
                                //retrieving sockets
                                ConsumerManager consumer = new ConsumerManager(retrieveSockets(mcd.getFutureTasks()), arrayLength);
                                Object[] elements = consumer.getArray(Object.class);
                                boolean result = collectionStorage.update(r.getKey(), elements);
                                if (result) {
                                    logger.debug("collection with key " + r.getKey() + " was updated");
                                } else {
                                    logger.debug("collection with key " + r.getKey() + " wasn't updated");
                                }
                                objectOutputStream.writeObject(new Response(Boolean.valueOf(result)));
                                break;
                            }
                        }
                        break;
                    }
                    case DELETE: {
                        switch (r.getElement()) {
                            case SINGLE_OBJECT: {
                                boolean result = objectStorage.delete(r.getKey());
                                if (result) {
                                    logger.debug("object with key " + r.getKey() + " was deleted");
                                } else {
                                    logger.debug("object with key " + r.getKey() + " wasn't updated");
                                }
                                objectOutputStream.writeObject(new Response(Boolean.valueOf(result)));
                                break;
                            }
                            case ARRAY: {
                                boolean result = arrayStorage.delete(r.getKey());
                                if (result) {
                                    logger.debug("array with key " + r.getKey() + " was deleted");
                                } else {
                                    logger.debug("array with key " + r.getKey() + " wasn't updated");
                                }
                                objectOutputStream.writeObject(new Response(Boolean.valueOf(result)));
                                break;
                            }
                            case COLLECTION: {
                                boolean result = collectionStorage.delete(r.getKey());
                                if (result) {
                                    logger.debug("collection with key " + r.getKey() + " was deleted");
                                } else {
                                    logger.debug("collection with key " + r.getKey() + " wasn't updated");
                                }
                                objectOutputStream.writeObject(new Response(Boolean.valueOf(result)));
                                break;
                            }
                        }
                        break;
                    }
                    case SAVE_SESSION: {
                        objectStorage.endSession();
                        arrayStorage.endSession();
                        collectionStorage.endSession();
                        logger.info("Session was successfully saved");
                        objectOutputStream.writeObject(new Response(Boolean.valueOf(true)));
                        break;
                    }
                }
            } catch (SocketException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } finally {
                try {
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            return null;
        }
    }

    public static void main(String... args) {
        if (args.length == 1) {
            //args[0] - port
            Shard s = new Shard(Integer.valueOf(args[0]));
            s.start();
            Scanner sc = new Scanner(System.in);
            String str = "stop";
            while(true) {
                if (sc.nextLine().equals(str)) {
                    s.stop();
                    System.exit(0);
                }
            }
        } else {
            try {
                throw new IllegalAccessException("Shard didn't start because of wrong parameters");
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
    }
}
