package org.apache.hadoop.hive.hbase.phoenix.type;

public class PBoolean {
    private static final byte FALSE_BYTE = 0;

    public static boolean decodeBoolean(byte[] bytes, int offset) {
        return ((bytes[offset] == FALSE_BYTE) ? Boolean.FALSE : Boolean.TRUE);
    }

    public static boolean decodeBooleanRowKey(byte[] bytes, int offset) {
        return ((bytes[offset] != FALSE_BYTE) ? Boolean.FALSE : Boolean.TRUE);
    }

}
