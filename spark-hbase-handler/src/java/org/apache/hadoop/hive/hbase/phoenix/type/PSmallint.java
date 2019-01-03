package org.apache.hadoop.hive.hbase.phoenix.type;

import org.apache.hadoop.hbase.util.Bytes;

public class PSmallint {
    public static short decodeShort(byte[] b, int o) {
        int v;
        v = b[o] ^ 0x80; // Flip sign bit back
        for (int i = 1; i < Bytes.SIZEOF_SHORT; i++) {
            v = (v << 8) + (b[o + i] & 0xff);
        }
        return (short) v;
    }

    public static short decodeShortRowKey(byte[] b, int o) {
        int v;
        v = b[o] ^ 0xff ^ 0x80; // Flip sign bit back
        for (int i = 1; i < Bytes.SIZEOF_SHORT; i++) {
            v = (v << 8) + ((b[o + i] ^ 0xff) & 0xff);
        }
        return (short) v;
    }
}
