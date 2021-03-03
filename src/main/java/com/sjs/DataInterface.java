package com.sjs;

public interface DataInterface {
    int size();

    boolean deserialize(byte[] buffer, int offset);
    void updateTimestamp();
}
