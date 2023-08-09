package com.geosot.javademo.units;
public class Z3 {
    private static final int MAX_BITS = 21;
    private static final long MAX_MASK = 0x1FFFFFL;
    private static final int MAX_DIM = 3;

    private long z;

    public Z3(long z) {
        this.z = z;
    }

    public long getZ() {
        return z;
    }

    public static long split(long value) {
        long x = value & MAX_MASK;
        x = (x | x << 32) & 0x1F00000000FFFFL;
        x = (x | x << 16) & 0x1F0000FF0000FFL;
        x = (x | x << 8) & 0x100F00F00F00F00FL;
        x = (x | x << 4) & 0x10C30C30C30C30C3L;
        return (x | x << 2) & 0x1249249249249249L;
    }

    public static int combine(long z) {
        long x = z & 0x1249249249249249L;
        x = (x ^ (x >> 2)) & 0x10C30C30C30C30C3L;
        x = (x ^ (x >> 4)) & 0x100F00F00F00F00FL;
        x = (x ^ (x >> 8)) & 0x1F0000FF0000FFL;
        x = (x ^ (x >> 16)) & 0x1F00000000FFFFL;
        x = (x ^ (x >> 32)) & MAX_MASK;
        return (int) x;
    }

    public static Z3 encode(int x, int y, int z) {
        long encodedValue = split(x) | (split(y) << 1) | (split(z) << 2);
        return new Z3(encodedValue);
    }

    public static void main(String[] args) {
        int x = 294;
        int y = 755;
        int z = 7;

        Z3 encoded = encode(x, y, z);
        System.out.println("Encoded value: " + encoded.getZ());

        int decodedX = combine(encoded.getZ());
        int decodedY = combine(encoded.getZ() >> 1);
        int decodedZ = combine(encoded.getZ() >> 2);

        System.out.println("Decoded X: " + decodedX);
        System.out.println("Decoded Y: " + decodedY);
        System.out.println("Decoded Z: " + decodedZ);
    }
}
