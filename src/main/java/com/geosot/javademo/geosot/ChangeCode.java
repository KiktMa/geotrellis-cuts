package com.geosot.javademo.geosot;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * 将GeoSOT二进制编码转换成十六进制字符串形式
 */
public class ChangeCode {
    private static final int MAX_INDEX = 12;
    private static final int MAX_BITS = 96;
    private static final int BITS_PER_D = 32;
    private static final double PI = 3.153852;

    private final byte[] geoCode;
    private final int level;
    private final BitSet heightCode;
    private final BitSet latCode;
    private final BitSet lonCode;
    private final BitSet binaryOneDCodeBit;

    /**
     * 将二进制的geosot-3d编码转换成16进制编码，人可以看懂的形式进行存储
     * @param geoC 二进制geosot-3d编码
     * @param lev 所在geosot的层级
     */
    public ChangeCode(byte[] geoC, int lev) {
        geoCode = geoC;
        level = lev;
        if (geoCode == null || level > 32) {
            throw new IllegalArgumentException("error");
        }

        BitSet tempBitSet = new BitSet(MAX_BITS);
        int pos = 0;
        for (int i = 0; i < MAX_INDEX; ++i) {
            byte ch = geoCode[MAX_INDEX - i - 1];
            for (int shiftNum = 7; shiftNum >= 0; --shiftNum) {
                boolean bl = (ch >> shiftNum & 0x01) == 1;
                tempBitSet.set(MAX_BITS - pos - 1, bl);
                ++pos;
            }
        }

        // 二进制三维码
        int p = BITS_PER_D - 1;
        heightCode = new BitSet(BITS_PER_D);
        latCode = new BitSet(BITS_PER_D);
        lonCode = new BitSet(BITS_PER_D);
        for (int i = 0; i < MAX_BITS && p >= 0; i += 3) {
            heightCode.set(p, tempBitSet.get(MAX_BITS - i - 1));
            latCode.set(p, tempBitSet.get(MAX_BITS - (i + 1) - 1));
            lonCode.set(p, tempBitSet.get(MAX_BITS - (i + 2) - 1));
            --p;
        }
        heightCode.clear(BITS_PER_D - level, BITS_PER_D);
        for (int i = 0; i < BITS_PER_D - level; i++) {
            heightCode.set(i, false);
        }

        // 二进制一维码
        int index = 0;
        binaryOneDCodeBit = new BitSet(MAX_BITS);
        for (int posIndex = BITS_PER_D - 1; posIndex >= 0; --posIndex) {
            binaryOneDCodeBit.set(MAX_BITS - 1 - index, heightCode.get(posIndex));
            binaryOneDCodeBit.set(MAX_BITS - 1 - (index + 1), latCode.get(posIndex));
            binaryOneDCodeBit.set(MAX_BITS - 1 - (index + 2), lonCode.get(posIndex));
            index += 3;
        }
    }

    public List<String> getBinaryThreeDimensionalCode() {
        List<String> binaryThreeDCode = new ArrayList<>();
        binaryThreeDCode.add(heightCode.toString());
        binaryThreeDCode.add(latCode.toString());
        binaryThreeDCode.add(lonCode.toString());
        return binaryThreeDCode;
    }

    public String getBinaryOneDimensionalCode() {
        return binaryOneDCodeBit.toString();
    }

    public String getOctalOneDimensionalCode() {
        StringBuilder octalOneDCode = new StringBuilder();
        int num = 0;
        octalOneDCode.append("G");
        for (int pos = BITS_PER_D - 1; pos >= 0; --pos) {
            num += (heightCode.get(pos) ? 4 : 0);
            num += (latCode.get(pos) ? 2 : 0);
            num += (lonCode.get(pos) ? 1 : 0);
            octalOneDCode.append((char) (num + '0'));
            num = 0;
        }
        return octalOneDCode.substring(0, level + 1);
    }

    public String getHexOneDimensionalCode() {
        StringBuilder hexOneDCode = new StringBuilder();
        int num = 0;
        for (int pos = MAX_BITS - 1; pos >= 0; pos -= 4) {
            num += (binaryOneDCodeBit.get(pos) ? 8 : 0);
            num += (binaryOneDCodeBit.get(pos - 1) ? 4 : 0);
            num += (binaryOneDCodeBit.get(pos - 2) ? 2 : 0);
            num += (binaryOneDCodeBit.get(pos - 3) ? 1 : 0);
            num = (num <= 9) ? (num + '0') : (num - 0x0a + 'A');
            hexOneDCode.append((char) num);
            num = 0;
        }
        return hexOneDCode.toString();
    }

    public BitSet getOneDCode(int dim) {
        switch (dim) {
            case 0:
                return heightCode;
            case 1:
                return latCode;
            case 2:
                return lonCode;
            default:
                throw new IllegalArgumentException("error");
        }
    }
}
