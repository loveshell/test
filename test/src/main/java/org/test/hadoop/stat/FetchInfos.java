package org.test.hadoop.stat;

import java.text.SimpleDateFormat;
import java.util.LinkedList;

public class FetchInfos {
	private static LinkedList<FetchInfo> fetchInfos = new LinkedList<FetchInfo>();
	static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public static FetchInfo getFetchInfo() {
		if (fetchInfos.size() == 10) {
			fetchInfos.removeFirst();
		}

		FetchInfo info = new FetchInfo();
		fetchInfos.add(info);
		return info;
	}

	public static String printString() {
		long totalBytes = 0;
		long start = 0;
		long end = 0;
		String str = "job间的间隔: ";
		for (int i = 0; i < fetchInfos.size(); i++) {
			FetchInfo info = fetchInfos.get(i);
			totalBytes += info.downBytes;
			if (i == 0)
				start = info.start;
			else {
				str += ((info.start - fetchInfos.get(i - 1).end) / 1000 + "s,");
			}
			if (i == fetchInfos.size() - 1)
				end = info.end;
		}
		float speed = 0;
		if ((end - start) != 0)
			speed = 1000 * totalBytes / (end - start) / 1024;
		return "######################################\n" + "time:" + sdf.format(start) + " - " + sdf.format(end)
				+ " avgSpeed=" + speed + "kbyte/s. " + str + "\n";
	}

	@Override
	public String toString() {
		return printString();
	}
}
