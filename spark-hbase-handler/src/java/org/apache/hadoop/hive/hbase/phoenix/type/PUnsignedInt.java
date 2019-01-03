package org.apache.hadoop.hive.hbase.phoenix.type;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.util.Bytes;

public class PUnsignedInt {
    public static int decodeInt(byte[] b, int o){
        return Bytes.toInt(b,o);
    }

    public static int decodeIntRowKey(byte[] b, int o){
        b = invert(b, o, new byte[Bytes.SIZEOF_INT], 0, Bytes.SIZEOF_INT);
        o = 0;
        return Bytes.toInt(b,o);
    }
    public static byte[] invert(byte[] src, int srcOffset, byte[] dest, int dstOffset, int length) {
        Preconditions.checkNotNull(src);
        Preconditions.checkNotNull(dest);
        for (int i = 0; i < length; i++) {
            dest[dstOffset + i] = (byte) (src[srcOffset + i] ^ 0xFF);
        }
        return dest;
    }
}
