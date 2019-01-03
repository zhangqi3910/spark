package org.apache.hadoop.hive.hbase.phoenix;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.hbase.phoenix.type.*;

import java.math.BigDecimal;
import java.util.Objects;

public class PhoenixHbaseDataTypeHandle {
    public static byte[] toHbaseByte(byte[] phoenixValue, String dataType) {
        if (phoenixValue.length == 0) {
            return phoenixValue;
        }
        if (Objects.equals(PhDataType.getPhDataType(dataType), PhDataType.BIGINT)) {
            return Bytes.toBytes(PLong.decodeLong(phoenixValue, 0));
        } else if (Objects.equals(PhDataType.getPhDataType(dataType), PhDataType.INT)) {
            return Bytes.toBytes(Integer.toString(PInt.decodeColumn(phoenixValue, 0)));
        } else if (Objects.equals(PhDataType.getPhDataType(dataType), PhDataType.STRING)
                || dataType.toLowerCase().contains(PhDataType.VARCHAR.getDataType().toLowerCase())) {
            return phoenixValue;
        } else if (Objects.equals(PhDataType.getPhDataType(dataType), PhDataType.UNSIGNED_INT)) {
            return Bytes.toBytes(PUnsignedInt.decodeInt(phoenixValue, 0));
        } else if (Objects.equals(PhDataType.getPhDataType(dataType), PhDataType.TINYINT)) {
            byte[] tmpByte = new byte[1];
            tmpByte[0] = PTinyint.decodeByte(phoenixValue, 0);
            return tmpByte;
        } else if (Objects.equals(PhDataType.getPhDataType(dataType), PhDataType.SMALLINT)) {
            return Bytes.toBytes(PSmallint.decodeShort(phoenixValue, 0));
        } else if (Objects.equals(PhDataType.getPhDataType(dataType), PhDataType.FLOAT)) {
            return Bytes.toBytes(PFloat.decodeFloat(phoenixValue, 0));
        } else if (Objects.equals(PhDataType.getPhDataType(dataType), PhDataType.DOUBLE)) {
            return Bytes.toBytes(PDouble.decodeDouble(phoenixValue, 0));
        } else if (Objects.equals(PhDataType.getPhDataType(dataType), PhDataType.BOOLEAN)) {
            return Bytes.toBytes(PBoolean.decodeBoolean(phoenixValue, 0));
        } else if (dataType.toLowerCase().contains(PhDataType.CHAR.getDataType().toLowerCase())) {
            return Bytes.toBytes(PCHAR.decodeCHAR(phoenixValue, 0));
        } else if (dataType.equalsIgnoreCase(PhDataType.TIMESTAMP.getDataType())) {
            return Bytes.toBytes(PTIMESTAMP.decodeTIMESTAMP(phoenixValue, 0));
        } else if (dataType.equalsIgnoreCase(PhDataType.BINARY.getDataType())) {
            return PBINARY.decodeBINARY(phoenixValue, 0);
        } else if (dataType.equalsIgnoreCase(PhDataType.DATE.getDataType())) {
            return Bytes.toBytes(PDATE.decodeDATE(phoenixValue, 0));
        } else if (dataType.split("\\(")[0].equalsIgnoreCase(PhDataType.DECIMAL.getDataType())) {
        	BigDecimal v = PDECIMAL.decodeDecimal(phoenixValue, 0);
        	if(v.scale() == 0) {
        		return Bytes.toBytes(v.toString());
        	} else {
        		return Bytes.toBytes(v.toString());
        	}
        }
        return phoenixValue;
    }

    public static byte[] toHbaseByteMRowKey(byte[] phoenixValue, String dataType) {
        if (phoenixValue.length == 0) {
            return phoenixValue;
        }
        if (Objects.equals(PhDataType.getPhDataType(dataType), PhDataType.INT)) {
            return Bytes.toBytes(PInt.decodeColumnKey(phoenixValue, 0));
        }
        return phoenixValue;
    }

    public static byte[] toHbaseByte(byte[] phoenixValue, String dataType, int i) {
        if (phoenixValue.length == 0) {
            return phoenixValue;
        }
        if (Objects.equals(PhDataType.getPhDataType(dataType), PhDataType.INT)) {
            return Bytes.toBytes(PInt.decodeColumnKey(phoenixValue, 0, i));
        }
        return phoenixValue;
    }

    public static byte[] toHbaseByteRwoKey(byte[] phoenixValue, String dataType) {
        if (phoenixValue.length == 0) {
            return phoenixValue;
        }
        if (Objects.equals(PhDataType.getPhDataType(dataType), PhDataType.BIGINT)) {
            return Bytes.toBytes(PLong.decodeLongRowKey(phoenixValue, 0));
        } else if (Objects.equals(PhDataType.getPhDataType(dataType), PhDataType.INT)) {
            return Bytes.toBytes(Integer.toString(PInt.decodeInt(phoenixValue, 0)));
        } else if (Objects.equals(PhDataType.getPhDataType(dataType), PhDataType.STRING)
                || dataType.toLowerCase().contains(PhDataType.VARCHAR.getDataType().toLowerCase())) {
            return Bytes.toBytes(PVARCHAR.decodeVARCHAR(phoenixValue, 0));
        } else if (Objects.equals(PhDataType.getPhDataType(dataType), PhDataType.UNSIGNED_INT)) {
            return Bytes.toBytes(PUnsignedInt.decodeIntRowKey(phoenixValue, 0));
        } else if (Objects.equals(PhDataType.getPhDataType(dataType), PhDataType.TINYINT)) {
            byte[] tmpByte = new byte[1];
            tmpByte[0] = PTinyint.decodeByteRowKey(phoenixValue, 0);
            return tmpByte;
        } else if (Objects.equals(PhDataType.getPhDataType(dataType), PhDataType.SMALLINT)) {
            return Bytes.toBytes(PSmallint.decodeShortRowKey(phoenixValue, 0));
        } else if (Objects.equals(PhDataType.getPhDataType(dataType), PhDataType.FLOAT)) {
            return Bytes.toBytes(PFloat.decodeFloatRowKey(phoenixValue, 0));
        } else if (Objects.equals(PhDataType.getPhDataType(dataType), PhDataType.DOUBLE)) {
            return Bytes.toBytes(PDouble.decodeDoubleRowKey(phoenixValue, 0));
        } else if (Objects.equals(PhDataType.getPhDataType(dataType), PhDataType.BOOLEAN)) {
            return Bytes.toBytes(PBoolean.decodeBooleanRowKey(phoenixValue, 0));
        } else if (dataType.toLowerCase().contains(PhDataType.CHAR.getDataType().toLowerCase())) {
            return Bytes.toBytes(PCHAR.decodeCHARRowKey(phoenixValue, 0));
        } else if (dataType.equalsIgnoreCase(PhDataType.TIMESTAMP.getDataType())) {
            return Bytes.toBytes(PTIMESTAMP.decodeTIMESTAMPRowKey(phoenixValue, 0));
        } else if (dataType.equalsIgnoreCase(PhDataType.BINARY.getDataType())) {
            return PBINARY.decodeBINARYRowKey(phoenixValue, 0);
        } else if (dataType.equalsIgnoreCase(PhDataType.DATE.getDataType())) {
            return Bytes.toBytes(PDATE.decodeDATERowKey(phoenixValue, 0));
        } else if (dataType.split("\\(")[0].equalsIgnoreCase(PhDataType.DECIMAL.getDataType())) {
        	BigDecimal v = PDECIMAL.decodeDecimalRowKey(phoenixValue, 0);
        	if(v.scale() == 0) {
        		return Bytes.toBytes(String.valueOf(v.longValue()));
        	} else {
        		return Bytes.toBytes(String.valueOf(v.doubleValue()));
        	}
        }
        return phoenixValue;
    }
}