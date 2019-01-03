package org.apache.hadoop.hive.hbase.phoenix.type;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.math.LongMath;

public class PDECIMAL {    
    protected static final byte ZERO_BYTE = (byte)0x80;
    protected static final byte NEG_TERMINAL_BYTE = (byte)102;
    protected static final int EXP_BYTE_OFFSET = 65;
    protected static final int POS_DIGIT_OFFSET = 1;
    protected static final int NEG_DIGIT_OFFSET = 101;
    protected static final long MAX_LONG_FOR_DESERIALIZE = Long.MAX_VALUE / 1000;
    protected static final BigInteger ONE_HUNDRED = BigInteger.valueOf(100);
    private static final byte DESC_SEPARATOR_BYTE = (byte) (0 ^ 0xFF);
    
    protected static BigDecimal toBigDecimal(byte[] bytes, int offset, int length) {
        // From exponent byte back to scale: (<exponent byte> & 0x7F) - 65) * 2
        // For example, (((-63 & 0x7F) - 65) & 0xFF) * 2 = 0
        // Another example: ((-64 & 0x7F) - 65) * 2 = -2 (then swap the sign for the scale)
        // If number is negative, going from exponent byte back to scale: (byte)((~<exponent byte> - 65 - 128) * 2)
        // For example: new BigDecimal(new BigInteger("-1"), -2);
        // (byte)((~61 - 65 - 128) * 2) = 2, so scale is -2
        // Potentially, when switching back, the scale can be added by one and the trailing zero dropped
        // For digits, just do a mod 100 on the BigInteger. Use long if BigInteger fits
        if (length == 1 && bytes[offset] == ZERO_BYTE) { return BigDecimal.ZERO; }
        int signum = ((bytes[offset] & 0x80) == 0) ? -1 : 1;
        int scale;
        int index;
        int digitOffset;
        long multiplier = 100L;
        int begIndex = offset + 1;
        if (signum == 1) {
            scale = (byte)(((bytes[offset] & 0x7F) - 65) * -2);
            index = offset + length;
            digitOffset = POS_DIGIT_OFFSET;
        } else {
            scale = (byte)((~bytes[offset] - 65 - 128) * -2);
            index = offset + length - (bytes[offset + length - 1] == NEG_TERMINAL_BYTE ? 1 : 0);
            digitOffset = -NEG_DIGIT_OFFSET;
        }
        length = index - offset;
        long l = signum * bytes[--index] - digitOffset;
        if (l % 10 == 0) { // trailing zero
            scale--; // drop trailing zero and compensate in the scale
            l /= 10;
            multiplier = 10;
        }
        // Use long arithmetic for as long as we can
        while (index > begIndex) {
            if (l >= MAX_LONG_FOR_DESERIALIZE || multiplier >= Long.MAX_VALUE / 100) {
                multiplier = LongMath.divide(multiplier, 100L, RoundingMode.UNNECESSARY);
                break; // Exit loop early so we don't overflow our multiplier
            }
            int digit100 = signum * bytes[--index] - digitOffset;
            l += digit100 * multiplier;
            multiplier = LongMath.checkedMultiply(multiplier, 100);
        }

        BigInteger bi;
        // If still more digits, switch to BigInteger arithmetic
        if (index > begIndex) {
            bi = BigInteger.valueOf(l);
            BigInteger biMultiplier = BigInteger.valueOf(multiplier).multiply(ONE_HUNDRED);
            do {
                int digit100 = signum * bytes[--index] - digitOffset;
                bi = bi.add(biMultiplier.multiply(BigInteger.valueOf(digit100)));
                biMultiplier = biMultiplier.multiply(ONE_HUNDRED);
            } while (index > begIndex);
            if (signum == -1) {
                bi = bi.negate();
            }
        } else {
            bi = BigInteger.valueOf(l * signum);
        }
        // Update the scale based on the precision
        scale += (length - 2) * 2;
        BigDecimal v = new BigDecimal(bi, scale);
        return v;
    }
    
    public static byte[] invert(byte[] src, int srcOffset, byte[] dest, int dstOffset, int length) {
		for (int i = 0; i < length; i++) {
			dest[dstOffset + i] = (byte) (src[srcOffset + i] ^ 0xFF);
		}
		return dest;
	}
    
    public static int getLength(byte[] keyBuffer, int keyOffset, int maxOffset) {
    	return maxOffset - keyOffset - (keyBuffer[maxOffset-1] == DESC_SEPARATOR_BYTE ? 1 : 0);
    }
    
	public static BigDecimal decodeDecimalRowKey(byte[] phoenixValue, int offset) {
		// TODO Auto-generated method stub
//		int len = getLength(phoenixValue, offset, phoenixValue.length);
//		byte [] dst = invert(phoenixValue, offset, new byte[len], 0, len);
//		return toBigDecimal(dst, 0, dst.length);
        return toBigDecimal(phoenixValue, offset, phoenixValue.length);
	}
    
	public static BigDecimal decodeDecimal(byte[] phoenixValue, int offset) {
		// TODO Auto-generated method stub
		return toBigDecimal(phoenixValue, offset, phoenixValue.length);
	}
}
