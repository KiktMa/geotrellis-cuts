package com.geosot.javademo.geosot;

import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Coordinate;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;

import java.io.*;
import java.text.DecimalFormat;
import java.util.Arrays;

public class CoordinateChange {

    public static void main(String[] args) {
        // x=343.814+399994  y=309.04+3138190
        // x=10238645.365    y=3197833.237
        // 偏移量：x=399994  y=3138190  z=44.8001
        //read2Code();
        Double[] coordinate1 = getCoordinate(3.59222, 3.59667);
        Double[] coordinate2 = getCoordinate(27.1167, 27.1211);
//        System.out.println("["+ (coordinate1[0] - 399994) +","+"]"+"["+ (coordinate1[1] - 399994) +"]");
//        System.out.println("["+ (coordinate1[1] - 3138190) +","+"]"+"["+ (coordinate1[1] - 3138190) +"]");
        System.out.println(Arrays.toString(coordinate1));
        System.out.println(Arrays.toString(coordinate2));
    }

    public static void read2Code(){
        try {
            File inputFile = new File("D:\\test_tif\\lonlat\\input1.txt");
            BufferedReader reader = new BufferedReader(new FileReader(inputFile));

            File outputFile = new File("D:\\test_tif\\lonlat\\lonlat222.txt");
            BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));

            String line;
            while ((line = reader.readLine()) != null) {
                String[] elements = line.split(" ");
                double x = Double.parseDouble(elements[0]);
                double y = Double.parseDouble(elements[1]);
                double high = Double.parseDouble(elements[2]);
                Double[] coordinate = getCoordinate(x+399994, y+3138190);
                String hexCode = GeoSOT.INSTANCE.getHexCode(coordinate[0], coordinate[1], high+44.8001, 23);

                writer.write(hexCode+"\n");
                GeoSOT.INSTANCE.freeMemory(hexCode);
            }

            reader.close();
            writer.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * 坐标转换
     * @param x
     * @param y
     * @return   返回转换后的坐标
     */
    public static Double[] getCoordinate(Double x,Double y) {
        Double[] res = new Double[2];
        Coordinate tar = null;
        try {
            Coordinate sour = new Coordinate(x, y);
            //这里要选择转换的坐标系是可以随意更换的,4326即为经纬度坐标系
            CoordinateReferenceSystem source = CRS.decode("EPSG:4326");
            CoordinateReferenceSystem target = CRS.decode("EPSG:"+3857);

            MathTransform transform = CRS.findMathTransform(source, target, true);
            tar = new Coordinate();
            //转换
            JTS.transform(sour, tar, transform);
        } catch (FactoryException | org.opengis.referencing.operation.TransformException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        String[] split = (tar.toString().substring(1, tar.toString().length() - 1)).split(",");
        //经纬度精度
        DecimalFormat fm = new DecimalFormat("0.0000000");
        res[0] = Double.valueOf(fm.format(Double.valueOf(split[0])));
        res[1] = Double.valueOf(fm.format(Double.valueOf(split[1])));
        return res;
    }
}
