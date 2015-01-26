package com.cybervision.javacourses.multithreading;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

/**
 * Created by Grigoriy on 1/17/2015.
 */
public class SupplierManager {

    private ExecutorService executorService;
    private Object[] array;

    private Logger logger;

    private FutureTask<Socket>[] socketFutureTasks;
    private Socket[] sockets;

    public SupplierManager(Object[] array, FutureTask<Socket>[] socketFutureTasks) {
        this.array = array;
        this.socketFutureTasks = socketFutureTasks;
        this.executorService = Executors.newFixedThreadPool(socketFutureTasks.length);
        setupLogging();
        startAsFutureTask();
        logger.info("array with length " + array.length + "is going to be received with [" + socketFutureTasks.length + " threads");
    }

    public SupplierManager(Object[] array, Socket[] sockets) {
        this.array = array;
        this.sockets = sockets;
        this.executorService = Executors.newFixedThreadPool(sockets.length);
        setupLogging();
        start();
        logger.info("array with length " + array.length + " is going to be received with [" + sockets.length + "] threads");
    }

    private void setupLogging() {
        this.logger = org.apache.log4j.Logger.getLogger(SupplierManager.class.getName());
        InputStream in = this.getClass().getClassLoader().getResourceAsStream("config/log4j.properties");
        Properties properties = new Properties();
        try {
            properties.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
        PropertyConfigurator.configure(properties);
    }

    private void start() {
        int capacity = array.length / sockets.length;
        for (int i = 0; i < sockets.length - 1; i++) {
            executorService.execute(new Supplier(sockets[i], array, i * capacity, (i + 1) * capacity, logger));
        }
        executorService.execute(new Supplier(sockets[sockets.length - 1],
                array, (sockets.length - 1) * capacity, array.length, logger));
        executorService.shutdown();
    }

    private void startAsFutureTask() {
        int capacity = array.length / socketFutureTasks.length;
        for (int i = 0; i < socketFutureTasks.length - 1; i++) {
            executorService.execute(new Supplier(socketFutureTasks[i], array, i * capacity, (i + 1) * capacity, logger));
        }
        executorService.execute(new Supplier(socketFutureTasks[socketFutureTasks.length - 1],
                array, (socketFutureTasks.length - 1) * capacity, array.length, logger));
        executorService.shutdown();
    }
}
