package org.apache.hadoop.hive.hbase.phoenix.type;

import org.apache.hadoop.hbase.util.Bytes;

import java.nio.ByteBuffer;

public class PInt {
    public static int decodeInt(byte[] bytes, int o) {
        int v;
        v = bytes[o] ^ 0xff ^ 0x80; // Flip sign bit back
        for (int i = 1; i < Bytes.SIZEOF_INT; i++) {
            v = (v << 8) + ((bytes[o + i] ^ 0xff) & 0xff);
        }
        return v;
    }

    public static int decodeColumn(byte[] bytes, int o) {
        int v;
        v = bytes[o] ^ 0x80; // Flip sign bit back
        for (int i = 1; i < Bytes.SIZEOF_INT; i++) {
            v = (v << 8) + (bytes[o + i] & 0xff);
        }
        return v;
    }

    public static int decodeColumnKey(byte[] bytes, int o) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(bytes.length);
        byteBuffer.put(bytes);
        byteBuffer.compact();
        byteBuffer.position(0);
        byte[] tmpBytes = new byte[4];
        byteBuffer.get(tmpBytes, 0, 4);
        return decodeColumn(tmpBytes, 0);
    }

    public static int decodeColumnKey(byte[] bytes, int o, int position) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(bytes.length);
        byteBuffer.put(bytes);
        byteBuffer.compact();
        byteBuffer.position((position - 1) * 4);
        byte[] tmpBytes = new byte[4];
        byteBuffer.get(tmpBytes, 0, 4);
        return decodeColumn(tmpBytes, 0);
    }
}
