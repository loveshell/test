package org.test.test;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class DNSCache {
	public static void main(String[] args) throws Exception {
		System.out.println("start loop\n");
		for (int i = 0; i < 10; ++i) {
			Date d = new Date();
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			System.out.println("current time:" + sdf.format(d));
			InetAddress addr1 = InetAddress.getByName("www.google.com");
			String addressCache = "addressCache";
			System.out.println(addressCache);
			printDNSCache(addressCache);
			System.out.println("getHostAddress:" + addr1.getHostAddress());
			System.out.println("*******************************************");
			System.out.println("\n");
			java.lang.Thread.sleep(31000);
		}

		System.out.println("end loop");
	}

	private static void printDNSCache(String cacheName) throws Exception {
		Class<InetAddress> klass = InetAddress.class;
		Field acf = klass.getDeclaredField(cacheName);
		acf.setAccessible(true);
		Object addressCache = acf.get(null);
		Class cacheKlass = addressCache.getClass();
		Field cf = cacheKlass.getDeclaredField("cache");
		cf.setAccessible(true);
		Map<String, Object> cache = (Map<String, Object>) cf.get(addressCache);
		for (Map.Entry<String, Object> hi : cache.entrySet()) {
			Object cacheEntry = hi.getValue();
			Class cacheEntryKlass = cacheEntry.getClass();
			Field expf = cacheEntryKlass.getDeclaredField("expiration");
			expf.setAccessible(true);
			long expires = (Long) expf.get(cacheEntry);

			Field af = cacheEntryKlass.getDeclaredField("address");
			af.setAccessible(true);
			InetAddress[] addresses = (InetAddress[]) af.get(cacheEntry);
			List<String> ads = new ArrayList<String>(addresses.length);
			for (InetAddress address : addresses) {
				ads.add(address.getHostAddress());
			}

			System.out.println(hi.getKey() + " " + new Date(expires) + " " + ads);
		}
	}
}