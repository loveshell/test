package org.test.hadoop.stat;

import java.util.LinkedList;

public class GenerateInfos {
	private static LinkedList<GenerateInfo> genInfos = new LinkedList<GenerateInfo>();
	public static long topn = 0;
	public static int hostn = 0;

	public static GenerateInfo getGenerateInfo() {
		if (genInfos.size() == 10) {
			genInfos.removeFirst();
		}

		GenerateInfo info = new GenerateInfo();
		genInfos.add(info);
		return info;
	}

	public static String printString() {
		String result = "######################################\n" + "generate: topn=" + topn + " hostn=" + hostn
				+ "\n";
		if (genInfos.size() > 0) {
			long max = 0;
			long min = Integer.MAX_VALUE;
			GenerateInfo maxInfo = null;
			GenerateInfo minInfo = null;

			for (int i = 0; i < genInfos.size(); i++) {
				GenerateInfo info = genInfos.get(i);
				if ((info.end - info.start) >= max) {
					max = info.end - info.start;
					maxInfo = info;
				}
				if ((info.end - info.start) <= min) {
					min = info.end - info.start;
					minInfo = info;
				}
			}
			String maxStr = "max: " + maxInfo + "\n";
			String minStr = "min: " + minInfo + "\n";
			return result + maxStr + minStr;
		} else
			return result;
	}

	@Override
	public String toString() {
		return printString();
	}
}
