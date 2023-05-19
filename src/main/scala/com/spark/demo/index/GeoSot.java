package com.spark.demo.index;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Pointer;
import com.sun.jna.Structure;

import java.util.List;

public interface GeoSot extends Library {
    GeoSot INSTANCE = Native.load("D:/JavaConsist/Project/geotrellis-cuts/src/main/resources/win32-x86-64/libGeoSOT3D", GeoSot.class);
    @Structure.FieldOrder({})
    class Point extends Structure {

        public static class ByReference extends Point implements Structure.ByReference {}
    }
//    void freeMemory(String code);
}