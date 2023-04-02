package com.spark.demo.geosot;

import com.sun.jna.Library;
import com.sun.jna.Native;

public interface GeoSot extends Library {
    GeoSot INSTANCE = (GeoSot)Native.load("GeoSOTForJava", GeoSot.class);

    String getHexCode(double var1, double var3, double var5, int var7);

    void freeMemory(String var1);
}

