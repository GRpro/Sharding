package com.cybervision.javacourses.multithreading;

import org.apache.log4j.Logger;

import java.io.*;
import java.net.Socket;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

/**
 * Created by Grigoriy on 1/17/2015.
 */
public class Consumer implements Callable<Object[]> {

    private FutureTask<Socket> socketFutureTask;
    private Socket socket;
    private Logger logger;

    public Consumer(FutureTask<Socket> socketFutureTask, Logger logger) {
        this.socketFutureTask = socketFutureTask;
        this.logger = logger;
    }

    public Consumer(Socket socket, Logger logger) {
        this.socket = socket;
        this.logger = logger;
    }

    private Socket retreiveSocket() {
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
    public Object[] call() throws Exception {
        Object[] part = null;
        try(Socket socket = retreiveSocket();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
            ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream())) {

            int number = (int) objectInputStream.readObject();
            part = new Object[number];
            for (int i = 0; i < number; i++) {
                part[i] = objectInputStream.readObject();
            }
            objectOutputStream.writeBoolean(true);
            logger.info("Consumer has got array with length " +
                    part.length + " from " + socket.getInetAddress() + ":" + socket.getPort());
        } catch (IOException e) {
//            e.printStackTrace();
        }
        return part;
    }
}
