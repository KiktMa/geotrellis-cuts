package com.geosot.javademo.geosot;

import com.sun.jna.Library;
import com.sun.jna.Native;

public interface GeoSOT extends Library {
    GeoSOT INSTANCE = Native.load("GeoSOTForJava", GeoSOT.class);

    String getHexCode(double lat, double lon, double height, int level);

    void freeMemory(String code);
}
