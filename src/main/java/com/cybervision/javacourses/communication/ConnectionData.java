package com.cybervision.javacourses.communication;

import java.io.Serializable;

public class ConnectionData implements Serializable {

    private static final long serialVersionUID = 3L;

    private String host;
    private int[] ports;
    private Object other;

    public ConnectionData(String host, int[] ports, Object other) {
        this.host = host;
        this.ports = ports;
        this.other = other;
    }

    public int[] getPorts() {
        return ports;
    }
    public String getHost() {
        return host;
    }
    public Object getOther() {return other;}
}
