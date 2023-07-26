package com.geosot.javademo.entity;

public class PointCloud {
    private String code;
    private double x;
    private double y;
    private double z;
    private double lon;
    private double lat;

    public PointCloud() {
    }

    public PointCloud(String code, double x, double y, double z, double lon, double lat) {
        this.code = code;
        this.x = x;
        this.y = y;
        this.z = z;
        this.lon = lon;
        this.lat = lat;
    }

    @Override
    public String toString() {
        return "PointCloud{" +
                "code='" + code + '\'' +
                ", x=" + x +
                ", y=" + y +
                ", z=" + z +
                ", lon=" + lon +
                ", lat=" + lat +
                '}';
    }
}
