package com.spark.demo.index;

import com.sun.jna.Library;
import com.sun.jna.Native;

public interface GeoSot extends Library {
    GeoSot INSTANCE = Native.load("D:/JavaConsist/Project/geotrellis-cuts/src/main/resources/win32-x86-64/libGeoSOT3D", GeoSot.class);
    int PointGridIdentify3D(double lat, double lon, double height, byte level, byte[] geoID);
    void freeMemory(String code);
}

