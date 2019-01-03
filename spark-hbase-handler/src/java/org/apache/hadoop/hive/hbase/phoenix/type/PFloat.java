package org.apache.hadoop.hive.hbase.phoenix.type;

import org.apache.hadoop.hbase.util.Bytes;

public class PFloat {
    public static float decodeFloat(byte[] b, int o){
        int value;
        value = Bytes.toInt(b, o);
        value--;
        value ^= (~value >> Integer.SIZE - 1) | Integer.MIN_VALUE;
        return Float.intBitsToFloat(value);
    }

    public static float decodeFloatRowKey(byte[] b, int o){
        int value;
        value = 0;
        for(int i = o; i < (o + Bytes.SIZEOF_INT); i++) {
            value <<= 8;
            value ^= (b[i] ^ 0xff) & 0xFF;
        }
        value--;
        value ^= (~value >> Integer.SIZE - 1) | Integer.MIN_VALUE;
        return Float.intBitsToFloat(value);
    }
}
