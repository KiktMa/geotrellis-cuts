package com.geosot.javademo.geosot;

import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Coordinate;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;

import java.text.DecimalFormat;
import java.util.Arrays;

public class CoordinateChange {

    public static void main(String[] args) {
        Double[] coordinate = getCoordinate(343.814, 309.04);
        System.out.println(Arrays.toString(coordinate));

        String hexCode = GeoSOT.INSTANCE.getHexCode(coordinate[0], coordinate[1], 37.1431, 24);
        System.out.println(hexCode);

        GeoSOT.INSTANCE.freeMemory(hexCode);
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
            CoordinateReferenceSystem source = CRS.decode("EPSG:3857");
            CoordinateReferenceSystem target = CRS.decode("EPSG:"+4326);

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
