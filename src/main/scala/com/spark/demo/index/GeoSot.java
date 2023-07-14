package com.spark.demo.index;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Structure;

public interface GeoSot extends Library {
    GeoSot INSTANCE = Native.load("D:/JavaConsist/Project/geotrellis-cuts/src/main/resources/win32-x86-64/libGeoSOT3D", GeoSot.class);
    /**
     * 输入3D剖分编码处理 给定经纬高度，指定层级，给出该层级下的编码
     * @param lat 维度
     * @param lon 经度
     * @param height 高度 单位米
     * @param level 层级
     * @param[out] geoID 三维编码
     * @returns 返回值为 -1时，层级越界，编码失败 ，返回值为0时，编码正常
     **/
    int PointGridIdentify3D(double lat, double lon, double height, byte level, byte[] geoID);

    /**
     * 网格编码转换为网格的经纬高范围
     * @param[in] geoID 空间网格编码
     * @param[in] layer 层级
     * @param[out] dLon 绝对值最小的经度值
     * @param[out] dLat 绝对值最小的纬度值
     * @param[out] dHeight 绝对值最小的高度值
     * @param[out] dLonMax 绝对值最大的经度值
     * @param[out] dLatMax 绝对值最大的纬度值
     * @param[out] dHeightM 绝对值最大的高度值
     **/
    void GetRangeFromCellCode3D(byte[] geoID, byte level, double lon, double lat, double height, double maxlon, double maxlat, double maxheight);

    @Structure.FieldOrder({"x", "y", "dir"})
    class Point extends Structure {
        public double x;
        public double y;
        public int dir; // 0：随意方向；1：上下方向；2：左右方向\process\CapacityProcess.java:37:46

        public static class ByReference extends Point implements Structure.ByReference {}
    }
//    void freeMemory(String code);
}

