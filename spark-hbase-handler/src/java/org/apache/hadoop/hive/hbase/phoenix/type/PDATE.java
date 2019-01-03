package org.apache.hadoop.hive.hbase.phoenix.type;

import java.text.SimpleDateFormat;
import java.util.Date;

public class PDATE {
    private static String timeStampFormat = "yyyy-MM-dd";

    private static SimpleDateFormat simpleDateFormat = new SimpleDateFormat(timeStampFormat);

    public static String decodeDATERowKey(byte[] bytes, int o) {
        return simpleDateFormat.format(new Date(PLong.decodeLongRowKey(bytes, o) - 8 * 60 * 60 * 1000));
    }

    public static String decodeDATE(byte[] bytes, int o) {
        return simpleDateFormat.format(new Date(PLong.decodeLong(bytes, o) - 8 * 60 * 60 * 1000));
    }
}
