package org.test.hadoop.stat;

import java.text.SimpleDateFormat;
import java.util.LinkedList;

public class ParseInfos {
	private static LinkedList<ParseInfo> parseInfos = new LinkedList<ParseInfo>();
	static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public static ParseInfo getParseInfo() {
		if (parseInfos.size() == 10) {
			parseInfos.removeFirst();
		}

		ParseInfo info = new ParseInfo();
		parseInfos.add(info);
		return info;
	}

	public static String printString() {
		long totalFingerTime = 0;
		long totalParseSuccess = 0;
		long start = 0;
		long end = 0;
		for (int i = 0; i < parseInfos.size(); i++) {
			ParseInfo info = parseInfos.get(i);
			totalFingerTime += info.fingerTime;
			totalParseSuccess += info.parseSuccess;
			if (i == 0)
				start = info.start;
			if (i == parseInfos.size() - 1)
				end = info.end;
		}
		float avgFingerTime = 0;
		if (totalParseSuccess != 0) {
			avgFingerTime = 100 * totalFingerTime / totalParseSuccess;
			avgFingerTime = avgFingerTime / 100;
		}
		return "######################################\n" + "time:" + sdf.format(start) + " - " + sdf.format(end)
				+ " avgFingerTime=" + avgFingerTime + "/ms.\n";
	}

	@Override
	public String toString() {
		return printString();
	}
}
