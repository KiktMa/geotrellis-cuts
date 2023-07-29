package com.spark.demo.test;

import com.geosot.javademo.geosot.ChangeCode;
import com.geosot.javademo.geosot.GeoSOT;
import com.spark.demo.index.GeoSot;
import org.junit.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.geosot.javademo.units.TxtFileProcessor.byteArrayToBinaryArray;
import static com.geosot.javademo.units.TxtFileProcessor.hexStringToByteArray;

public class Count {
    @Test
    public void getCode() {
        byte level = 18;
        byte[] geoID = new byte[32];
        GeoSot.INSTANCE.PointGridIdentify3D(27.6878, 91.7589, 0, level, geoID);
        ChangeCode changeCode = new ChangeCode(geoID, level);
        String binaryOneDimensionalCode = changeCode.getHexOneDimensionalCode();
        System.out.println(binaryOneDimensionalCode);
        System.out.println("验证两种获取geosot编码方式是否相同");

        String hexCode = GeoSOT.INSTANCE.getHexCode(27.6878, 91.7589, 0, 18);
        if(binaryOneDimensionalCode.equals(hexCode)){
            System.out.println("true");
        }else System.out.println("false");
        GeoSOT.INSTANCE.freeMemory(hexCode);
    }

    // 编码解析：0004836404D90C009A61A6C1
    @Test
    public void codeExplain(){
//        byte[] byteArray = hexStringToByteArray("0004836404D90C009A61A6C1");
//        boolean[] binaryArray = byteArrayToBinaryArray(byteArray);
//        // 打印二进制数组
//        for (boolean bit : binaryArray) {
//            System.out.print(bit ? "1" : "0");
//        }
        double lon = 0.0;
        double lat = 0.0;
        double height = 0.0;
        double maxlon = 0.0;
        double maxlat = 0.0;
        double maxhight = 0.0;
//        GeoSot.INSTANCE.GetRangeFromCellCode3D("000000000000010010000011011001000000010011011001000011000000000010011010011000011010011011000001".getBytes(), (byte) 32,lon,lat,height,maxlon,maxlat,maxhight);
//        GeoSot.INSTANCE.GetRangeFromCellCode3D("0004836404D90C009A61A6C1".getBytes(),(byte) 32,lon,lat,height,maxlon,maxlat,maxhight);
//        GeoSot.INSTANCE.getRangeFromCellCode3D("000000000000010010000011011001000000010011011001000011000000000010011010011000011010011011000001".getBytes(), (byte) 32,lon,lat,height,maxlon,maxlat,maxhight);
//        System.out.println(lon);
//        System.out.println(lat);
//        System.out.println(height);
//        System.out.println(maxlon);
//        System.out.println(maxlat);
//        System.out.println(maxhight);
    }
}
