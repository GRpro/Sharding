package com.cybervision.javacourses.communication.meta;

import java.io.Serializable;

/**
 * Created by Grigoriy on 1/2/2015.
 */
public class MetaResponse implements Serializable {

    static final long serialVersionUID = 2L;

    private String host;
    private int port;

    public MetaResponse(int port, String host) {
        this.port = port;
        this.host = host;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
}
