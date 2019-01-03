package org.apache.hadoop.hive.hbase.phoenix.type;

import org.apache.hadoop.hbase.util.Bytes;

public class PCHAR {

    public static final byte SPACE_UTF8 = 0x20;

    public static final byte INVERTED_SPACE_UTF8 = invert(new byte[]{SPACE_UTF8}, 0, new byte[1], 0, 1)[0];

    public static byte[] invert(byte[] src, int srcOffset, byte[] dest, int dstOffset, int length) {
        for (int i = 0; i < length; i++) {
            dest[dstOffset + i] = (byte) (src[srcOffset + i] ^ 0xFF);
        }
        return dest;
    }

    public static String decodeCHARRowKey(byte[] bytes, int offset) {
        int length = getUnpaddedCharLength(bytes, offset, bytes.length, false);
        byte[] dest = new byte[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            dest[i] = (byte) (bytes[i] ^ 0xFF);
        }
        offset = 0;
        return Bytes.toString(dest, offset, length).trim();
    }

    public static String decodeCHAR(byte[] bytes, int offset) {
        int length = getUnpaddedCharLength(bytes, offset, bytes.length, true);
        return Bytes.toString(bytes, offset, length).trim();
    }

    public static int getUnpaddedCharLength(byte[] b, int offset, int length, boolean order) {
        return getFirstNonBlankCharIdxFromEnd(b, offset, length, order) - offset + 1;
    }

    public static int getFirstNonBlankCharIdxFromEnd(byte[] string, int offset, int length, boolean order) {
        int i = offset + length - 1;
        byte space = order ? SPACE_UTF8 : INVERTED_SPACE_UTF8;
        for (; i >= offset; i--) {
            if (string[i] != space) {
                break;
            }
        }
        return i;
    }
}
