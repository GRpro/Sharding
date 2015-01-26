package com.cybervision.javacourses.communication;

import java.io.Serializable;

/**
 * Created by Grigoriy on 1/2/2015.
 */
public class Request implements Serializable {
    private static final long serialVersionUID = 1L;

    public static enum Operation {
        CREATE, READ, UPDATE, DELETE, SAVE_SESSION
    };

    public static enum Element {
        SINGLE_OBJECT, COLLECTION, ARRAY
    }

    private final Operation operation;
    private final Element element;
    private final Object data;
    private final Object key;

    public Request(Operation operation, Element element, Object data, Object key) {
        this.operation = operation;
        this.element = element;
        this.data = data;
        this.key = key;
    }

    public Operation getOperation() {
        return operation;
    }

    public Element getElement() {
        return element;
    }

    public Object getData() {
        return data;
    }

    public Object getKey() {
        return key;
    }
}
