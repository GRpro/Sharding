package com.cybervision.javacourses.master;

import com.cybervision.javacourses.communication.meta.MetaRequest;
import com.cybervision.javacourses.communication.meta.MetaResponse;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Properties;
import java.util.Scanner;

/**
 * Created by Grigoriy on 1/2/2015.
 */
public class Master implements Runnable {

    private ServerSocket serverSocket;
    private Thread thread;
    private boolean isAlive;

    private String[] hosts;
    private int[] ports;
    private int port;

    /**The count of shards*/
    private int num;

    private Logger logger;

    /**
     * Constructs master.
     * The elements in two arrays with the same indexes should be relational.
     * Each pare of port and host with the same indexes in arrays determine address of the special shard.
     * @param port Special port on which Master will be run
     * @param hosts Array of special hosts on which shards are running
     * @param ports Array of scecial ports on which shards are running
     */
    public Master(int port, String[] hosts, int[] ports) {
        //setup logging
        setupLogging();

        //setup other
        this.port = port;
        this.hosts = hosts;
        this.ports = ports;
        this.num = ports.length;
        this.thread = new Thread(this, "Master");
        try {
            serverSocket = new ServerSocket(port);
            logger.info("master is going to be run on port " + port);
            logger.info("localhost: " + InetAddress.getLocalHost());
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (int i = 0; i < ports.length; i++) {
            logger.info("host[" + i + "] = " + hosts[i] + " | " + " port[" + i + "] = " + ports[i]);
        }
    }

    private void setupLogging() {
        this.logger = Logger.getLogger(Master.class.getName());
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
        while (isAlive) {
            MetaRequest metaRequest;
            try (Socket socket = serverSocket.accept();
                 ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                 ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream())) {

                logger.trace("master connected");
                metaRequest = (MetaRequest) objectInputStream.readObject();
                logger.debug("master accepted MetaRequest with key " + metaRequest.getHashcode());
                MetaResponse metaResponse = function(metaRequest.getHashcode());
                objectOutputStream.writeObject(metaResponse);
                logger.debug("master sent MetaResponse with such parameters: host " + 
                        metaResponse.getHost() + " port " + metaResponse.getPort());
            }  catch (SocketException e) {
                logger.debug("socket connection was closed");
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    private MetaResponse function(int hashcode) {
        MetaResponse metaResponse = new MetaResponse(ports[Math.abs(hashcode % num)], hosts[Math.abs(hashcode % num)]);
        return metaResponse;
    }

    public void start() {
        isAlive = true;
        thread.start();
    }

    public void stop() {
        isAlive = false;
        try {
            serverSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("master was stopped");
    }

    private static void startConsoleUtil(Master m) {
        Scanner sc = new Scanner(System.in);
        String s = "stop";
        while(true) {
            if (sc.nextLine().equals(s)) {
                m.stop();
                System.exit(0);
            }
        }
    }

    public static void main(String[] args) {
        if (args.length != 0 && (args.length - 1) % 2 == 0) {
            int count = (args.length - 1) / 2;
            String[] hosts = new String[count];
            int[] ports = new int[count];
            for (int i = 0; i < count; i++) {
                hosts[i] = args[i * 2 + 1];
                ports[i] = Integer.valueOf(args[i * 2 + 2]);
            }

            Master m = new Master(Integer.valueOf(args[0]), hosts, ports);
            m.start();
            startConsoleUtil(m);
        } else {
            try {
                throw new IllegalAccessException("Master didn't start because of wrong parameters");
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
    }
}
