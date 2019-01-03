package org.apache.hadoop.hive.hbase.phoenix.type;

public class PTinyint {
    public static byte decodeByteRowKey(byte[] b, int o) {
        int v = b[o] ^ 0xff ^ 0x80;
        return (byte) v;
    }

    public static byte decodeByte(byte[] b, int o) {
        int v = b[o] ^ 0x80;
        return (byte) v;
    }
}
