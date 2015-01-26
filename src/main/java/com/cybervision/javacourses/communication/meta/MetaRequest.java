package com.cybervision.javacourses.communication.meta;

import java.io.Serializable;

/**
 * Created by Grigoriy on 1/2/2015.
 */
public class MetaRequest implements Serializable {
    static final long serialVersionUID = 1L;

    private int hashcode;

    public MetaRequest(int hashcode) {
        this.hashcode = hashcode;
    }

    public int getHashcode() {
        return hashcode;
    }

    public void setHashcode(int hashcode) {
        this.hashcode = hashcode;
    }
}
