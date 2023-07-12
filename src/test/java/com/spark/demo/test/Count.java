package com.spark.demo.test;

import com.geosot.javademo.geosot.ChangeCode;
import com.spark.demo.index.GeoSot;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Count {
    public static void main(String[] args) {
        byte level = 18;
        byte[] geoID = new byte[32];
        GeoSot.INSTANCE.PointGridIdentify3D(27.6878, 91.7589, 0, level, geoID);
        ChangeCode changeCode = new ChangeCode(geoID, level);
        String binaryOneDimensionalCode = changeCode.getHexOneDimensionalCode();
        System.out.println(binaryOneDimensionalCode);
    }
}
