package org.test.test;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Test {
	private static Lock dbLock = new ReentrantLock();

	public static void main(String[] args) throws Exception {
		// String a = "";
		// a += "dd";
		// System.out.println(a);
		// float a = 28;
		// float b = 8;
		// System.out.println(Math.round(a / b));
		// System.out.println(Arrays.asList(getHostSplits()));
		// System.out.println(getHost("http://www.sina.com"));

		String a = "attempt_201408261602_0068_m_000001_0".substring(26);
		System.out.println(a.substring(0, a.length() - 2));
	}

	public static String[] getHostSplits() {
		int numSplits = 27;
		String first = "a";
		String middlePre = "www.";
		char a = 'a';
		String[] splits = new String[numSplits];
		splits[0] = first;
		for (int i = 1; i < numSplits; i++) {
			splits[i] = middlePre + (a++);
		}

		return splits;
	}

	private static String getHost(String url) {
		String host = null;
		try {
			URL tmp = new URL(url);
			host = tmp.getHost();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
		return host;
	}

	public static void testWrite() throws Exception {

		System.out.println(Float.intBitsToFloat(Float.floatToIntBits(11.25f)));
	}

	public static void testPartition() throws Exception {
		String[] ips = { "10.200.5.227", "10.200.5.21", "10.200.4.232", "10.200.4.141", "10.200.4.196", "10.200.7.13",
				"10.200.4.249", "10.200.5.85", "10.200.5.17", "10.200.7.225", "10.200.4.136", "10.200.6.207",
				"xxhuang", "kfang-felton", "sjwu", "wtong", "kwu-ganymede", "qwang-forward", "hwei-mike", "mfang-nj",
				"zning", "qzhang-john", "zddu", "xjyan" };
		int seed = new Random().nextInt();
		Map<Integer, AtomicInteger> map = new HashMap<Integer, AtomicInteger>();
		Map<Integer, List> map0 = new HashMap<Integer, List>();
		Map<Integer, AtomicInteger> map2 = new HashMap<Integer, AtomicInteger>();
		for (int i = 0; i < ips.length; i++) {
			int hashCode = ips[i].hashCode();
			hashCode ^= seed;
			hashCode = (hashCode & Integer.MAX_VALUE);

			Integer part = new Integer(hashCode % 4);
			if (map.containsKey(part)) {
				map.get(part).incrementAndGet();
				map0.get(part).add(ips[i]);
			} else {
				map.put(part, new AtomicInteger(1));
				List list = new ArrayList();
				list.add(ips[i]);
				map0.put(part, list);
			}
			Integer part2 = new Integer((hashCode % (4 * 100 - 1)) % 4);
			if (map2.containsKey(part2)) {
				map2.get(part2).incrementAndGet();
			} else {
				map2.put(part2, new AtomicInteger(1));
			}
		}
		System.out.println(map);
		System.out.println(map0);
		// System.out.println(map2);
	}

	public static void testLock() throws InterruptedException {
		// System.out.println(0.0f > Float.NaN);
		// System.out.println(0.0f < Float.NaN);
		// System.out.println(0.0f == Float.NaN);

		// dbLock.lock();
		// dbLock.lock();
		// // dbLock.unlock();
		// dbLock.unlock();
		// System.out.println(dbLock.toString());
		//
		int i = 0;
		while (i < 3) {
			i++;
			System.out.println("continue do");
			dbLock.lock();
			try {

				continue;

			} catch (Exception e) {
				e.printStackTrace();

			} finally {
				dbLock.unlock();
				System.out.println("finally do ");
			}

			Thread.sleep(1000);
		}
	}

	private static void testComp() {
		int j = 0;
		System.out.println(j++);

		System.out.println(123 % 2);
		System.out.println(123 / 2);
		System.out.println(Math.pow(3, 2));

		for (int i = 0; i < 128; i++) {
			System.out.print(Character.valueOf((char) i));
		}
		System.out.println("");
		System.out
				.println("!#$%&()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[]^_`abcdefghijklmnopqrstuvwxyz{|}~"
						.length());
	}
}
