package com.cybervision.javacourses.multithreading;


import org.apache.log4j.Logger;

import java.io.*;
import java.net.Socket;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

/**
 * Created by Grigoriy on 1/17/2015.
 */
public class Supplier implements Runnable {

    private int first;
    private int last;
    private Object[] array;
    private FutureTask<Socket> socketFutureTask;
    private Socket socket;
    private Logger logger;

    public Supplier(FutureTask<Socket> socketFutureTask, Object[] array, int first, int last, Logger logger) {
        this.array = array;
        this.first = first;
        this.last = last;
        this.socketFutureTask = socketFutureTask;
        this.logger = logger;
        this.socket = null;
    }

    public Supplier(Socket socket, Object[] array, int first, int last, Logger logger) {
        this.array = array;
        this.first = first;
        this.last = last;
        this.socketFutureTask = null;
        this.logger = logger;
        this.socket = socket;
    }



    private Socket retrieveSocket() {
        if (socketFutureTask == null)
            return this.socket;
        try {
            return socketFutureTask.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void run() {
        try (Socket socket = retrieveSocket();
             ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
             ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream())) {
            objectOutputStream.writeObject(new Integer(last - first));
            for (int i = first; i < last; i++) {
                objectOutputStream.writeObject(array[i]);
            }
            objectOutputStream.flush();
            boolean b = objectInputStream.readBoolean();
            logger.trace("Supplier on port " + socket.getPort() + " received its array with length " + (last - first));
        } catch (IOException e) {
//            e.printStackTrace();
        }
    }
}
