package org.apache.hadoop.hive.hbase.phoenix.type;

import org.apache.hadoop.hbase.util.Bytes;

public class PVARCHAR {
    public static String decodeVARCHAR(byte[] bytes, int offset) {
        byte[] dest = new byte[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            dest[i] = (byte) (bytes[i] ^ 0xFF);
        }
        return Bytes.toString(dest, offset);
    }
}
