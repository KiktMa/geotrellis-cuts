package com.spark.demo.index;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Pointer;
import com.sun.jna.Structure;

import java.util.List;

public interface GeoSot extends Library {
    GeoSot INSTANCE = Native.load("D:/JavaConsist/Project/geotrellis-cuts/src/main/resources/win32-x86-64/libGeoSOT3D", GeoSot.class);
    int PointGridIdentify3D(double lat, double lon, double height, byte level, byte[] geoID);

    @Structure.FieldOrder({"x", "y", "dir"})
    class Point extends Structure {
        public double x;
        public double y;
        public int dir; // 0：随意方向；1：上下方向；2：左右方向\process\CapacityProcess.java:37:46

        public static class ByReference extends Point implements Structure.ByReference {}
    }
//    void freeMemory(String code);
}

