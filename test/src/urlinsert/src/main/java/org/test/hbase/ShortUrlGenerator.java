package org.test.hbase;

import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Map;

public class ShortUrlGenerator {
	public final static char[] chars62 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz".toCharArray();
	// private final static char[] chars91 =
	// "!#$%&()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[]^_`abcdefghijklmnopqrstuvwxyz{|}~"
	// .toCharArray();
	private final static char[] hexDigits = "0123456789abcdef".toCharArray();
	private final static String preKey = "2014new"; // 自定义生成MD5加密字符串前的混合KEY

	public static void main(String[] args) {
		// System.out.println(MD5Encode("askfjlaksdjflasfEEEFj"));

		testConflict();
	}

	private static void testConflict() {
		Map tmp = new HashMap();
		long start = System.currentTimeMillis();
		for (int ni = 1; ni <= 1; ni++) {
			for (int mi = 0; mi < 10000000; mi++) {
				String strRowId = "http://" + Integer.toString(mi) + "sina.com.cn/index" + Integer.toString(ni)
						+ ".html";
				// String ss = shortText(strRowId, 4);
				String ss = shortBy62(strRowId);
				if (tmp.containsKey(ss)) {
					System.out.println(ss);
					String add = tmp.get(ss) + "=" + strRowId;
					tmp.put(ss, add);
					System.out.println(add);
				} else
					tmp.put(ss, strRowId);
			}
		}
		long end = System.currentTimeMillis();
		System.out.println("time=" + (end - start));
	}

	public static String shortBy62(String in) {
		String hex = MD5Encode(preKey + in);

		String result = "";
		result += IDShorter.toShort(Long.valueOf(hex.substring(0, 15), 16), chars62);
		result += IDShorter.toShort(Long.valueOf(hex.substring(15, 30), 16), chars62);
		result += IDShorter.toShort(Long.valueOf(hex.substring(30, 32), 16), chars62);

		return result;
	}

	/**
	 * @param in
	 * @param splitCount
	 *            必须小于等于4
	 * @return
	 */
	private static String shortBySplit(String in, int splitCount) {
		if (in == null || in.length() == 0 || splitCount < 1)
			return null;
		String result = "";
		String[] tmp = shortBySplit(in);
		for (int i = 0; i < splitCount; i++) {
			result += tmp[i];
		}
		return result;
	}

	private static String[] shortBySplit(String in) {
		String hex = MD5Encode(preKey + in);
		int hexLen = hex.length();
		int baseCount = hexLen / 8;
		String[] result = new String[baseCount];

		for (int i = 0; i < baseCount; i++) {
			String outChars = "";
			String single = hex.substring(i * 8, (i + 1) * 8);
			long idx = Long.valueOf("3FFFFFFF", 16) & Long.valueOf(single, 16);

			for (int k = 0; k < 6; k++) {
				int index = (int) (Long.valueOf("0000003D", 16) & idx);
				outChars += chars62[index];
				idx = idx >> 5;
			}
			result[i] = outChars;
		}

		return result;
	}

	public static String MD5Encode(String in) {
		String r = null;
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			// System.out.println(Bytes.toString(md.digest(ori.trim().getBytes("UTF-8"))));
			r = byteArrayToHexString(md.digest(in.trim().getBytes("UTF-8")));
		} catch (Exception ex) {
		}
		return r;
	}

	public static String byteArrayToHexString(byte[] b) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < b.length; i++) {
			sb.append(byteToHexString(b[i]));
		}
		return sb.toString();
	}

	private static String byteToHexString(byte b) {
		int n = b;
		if (n < 0)
			n = 256 + n;
		int d1 = n / 16;
		int d2 = n % 16;
		return new StringBuilder().append(hexDigits[d1]).append(hexDigits[d2]).toString();
	}

	/**
	 * 62位编码
	 * 
	 * @author xxhuang
	 * 
	 */
	public static class IDShorter {
		public static void main(String[] args) {
			String tmp = toShort(62, chars62);
			System.out.println(tmp);
			System.out.println(unShort(tmp, chars62));
		}

		// 进行压缩
		public static String toShort(long n, char[] radix) {
			String result = "";
			int len = radix.length;
			while (n / len >= 1) {
				result = radix[(int) (n % len)] + result;
				n /= len;
			}
			result = radix[(int) n] + result;

			return result;
		}

		// 还原ID
		public static long unShort(String in, char[] radix) {
			long result = 0;
			if (in != null && in.length() > 0) {
				in = in.trim();
				int len = in.length();
				int digit = radix.length;
				for (int x = 0; x < len; x++) {
					result += org.apache.commons.lang.ArrayUtils.indexOf(radix, in.charAt(len - 1 - x))
							* (long) Math.pow(digit, x);
				}
			}

			return result;
		}
	}
}
