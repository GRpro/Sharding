package com.cybervision.javacourses.multithreading;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.net.Socket;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

/**
 * Created by Grigoriy on 1/18/2015.
 */
public class ConsumerManager<T> {

    private int arrayLength;
    private Logger logger;

    private FutureTask<Object[]> [] results;
    private ExecutorService executorService;

    private FutureTask<Socket>[] socketFutureTasks;
    private Socket[] sockets;

    /**
     * Constructor 1
     * @param socketFutureTasks
     * @param arrayLength
     */
    public ConsumerManager(FutureTask<Socket>[] socketFutureTasks, int arrayLength) {
        this.socketFutureTasks = socketFutureTasks;
        this.arrayLength = arrayLength;
        setupLogging();

        this.executorService = Executors.newFixedThreadPool(socketFutureTasks.length);
        this.results = new FutureTask[socketFutureTasks.length];
        //starting consumers
        int count = 0;
        for (FutureTask<Socket> futureTask : socketFutureTasks) {
            results[count] = new FutureTask<>(new Consumer(futureTask, logger));
            executorService.execute(results[count]);
            count++;
        }
    }

    /**
     * Constructor 2
     * @param sockets
     * @param arrayLength
     */
    public ConsumerManager(Socket[] sockets, int arrayLength) {
        this.sockets = sockets;
        this.arrayLength = arrayLength;
        setupLogging();

        this.executorService = Executors.newFixedThreadPool(sockets.length);
        this.results = new FutureTask[sockets.length];
        int count = 0;
        for (Socket socket : sockets) {
            results[count] = new FutureTask<>(new Consumer(socket, logger));
            executorService.execute(results[count]);
            count++;
        }

    }

    private void setupLogging() {
        this.logger = org.apache.log4j.Logger.getLogger(ConsumerManager.class.getName());
        InputStream in = this.getClass().getClassLoader().getResourceAsStream("config/log4j.properties");
        Properties properties = new Properties();
        try {
            properties.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
        PropertyConfigurator.configure(properties);
    }

    public T[] getArray(Class<T> cl) {

        //creating array with special type of elements T
        T[] result = (T[]) Array.newInstance(cl, arrayLength);

        int last = 0;
        Object[] part;
        for (int i = 0; i < results.length; i++) {
            try {
                int count = 0;
                part = results[i].get();

                for (int j = last; j < last + part.length; j++) {
                    result[j] = (T) part[count++];
                }
                last = last + part.length;

            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        executorService.shutdown();
        logger.trace("whole array with length: " + result.length + " is being returned");
        return result;
    }
}
