package com.geosot.javademo.geosot;

import com.sun.jna.Library;
import com.sun.jna.Native;

/**
 * 调用window下的dll库，但在Linux中无法运行，因此对其进行重写
 * 在scala目录下的com.spark.demo.index文件夹下的Geosot接口中
 * 在Linux中只需将libGeoSOT3D.dll换成libGeoSOT3D.so即可
 */
public interface GeoSOT extends Library {
    GeoSOT INSTANCE = Native.load("GeoSOTForJava", GeoSOT.class);

    String getHexCode(double lat, double lon, double height, int level);

    void freeMemory(String code);
}
