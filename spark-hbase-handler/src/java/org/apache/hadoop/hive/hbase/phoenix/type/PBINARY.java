package org.apache.hadoop.hive.hbase.phoenix.type;

public class PBINARY {
    public static byte[] decodeBINARY(byte[] bytes, int offset) {
        return bytes;
    }


    public static byte[] decodeBINARYRowKey(byte[] bytes, int offset) {
        byte[] bytesCopy = new byte[bytes.length];
        System.arraycopy(bytes, offset, bytesCopy, 0, bytes.length);
        for (int i = 0; i < bytes.length; i++) {
            bytesCopy[i] = (byte) (bytes[i] ^ 0xFF);
        }
        return bytesCopy;
    }
}
