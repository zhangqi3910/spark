package org.apache.hadoop.hive.hbase.phoenix.type;

import org.apache.hadoop.hbase.util.Bytes;

public class PLong {
    public static long decodeLongRowKey(byte[] bytes, int o) {
        byte b = bytes[o];
        long v;
        b = (byte) (b ^ 0xff);
        v = b ^ 0x80; // Flip sign bit back
        for (int i = 1; i < Bytes.SIZEOF_LONG; i++) {
            b = bytes[o + i];
            b ^= 0xff;
            v = (v << 8) + (b & 0xff);
        }
        return v;
    }


    public static long decodeLong(byte[] bytes, int o) {
        long v;
        byte b = bytes[o];
        v = b ^ 0x80; // Flip sign bit back
        for (int i = 1; i < Bytes.SIZEOF_LONG; i++) {
            b = bytes[o + i];
            v = (v << 8) + (b & 0xff);
        }
        return v;
    }
}
