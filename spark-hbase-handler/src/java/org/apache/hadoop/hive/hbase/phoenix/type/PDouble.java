package org.apache.hadoop.hive.hbase.phoenix.type;

import org.apache.hadoop.hbase.util.Bytes;

public class PDouble {
    public static double decodeDoubleRowKey(byte[] bytes, int o) {
        long l;
        l = 0;
        for (int i = o; i < o + Bytes.SIZEOF_LONG; i++) {
            l <<= 8;
            l ^= (bytes[i] ^ 0xff) & 0xFF;
        }
        l--;
        l ^= (~l >> Long.SIZE - 1) | Long.MIN_VALUE;
        return Double.longBitsToDouble(l);
    }

    public static double decodeDouble(byte[] bytes, int o) {
        long l;
        l = Bytes.toLong(bytes, o);
        l--;
        l ^= (~l >> Long.SIZE - 1) | Long.MIN_VALUE;
        return Double.longBitsToDouble(l);
    }
}
