package org.test.utils;

import java.io.File;
import java.util.ArrayList;

import org.test.bean.PatternString;

/**
 * 
 * @author hwei.mike
 * */
public class StringMaxMatch {
	static IOUtils rw_Utils = new IOUtils();

	static String path = "D:/新闻正文抓取/news/after/";
	static String[] paths = { "顶尖财经网/", "中国经济导报网/", "国家一类新闻网站/", "华广网", "華富財經Quamnet.com/", "中国文明网/", "中国证券网/", "证券时报网/" };

	public static void main(String[] args) {
		String p1 = "贝因美股价连涨获机构追捧 QFII或有对倒筹码嫌疑_公司信息_顶尖财经网";
		String p2 = "百润股份卖预调鸡尾酒 55.63亿揽进巴克斯酒业_公司信息_顶尖财经网";

		String s1 = rw_Utils.Read(new File(path + paths[0] + p1), "GBK");// "<title>1.32亿定增股“空袭”张化机_公司信息_顶尖财经网</title>";
		String s2 = rw_Utils.Read(new File(path + paths[0] + p2), "GBK");// "<title>A股迎来殡葬概念股福成五丰拟15亿收购公墓资产_公司信息_顶尖财经网</title>";
		rw_Utils.Write(p1, s1, "gbk", true);
		rw_Utils.Write(p2, s2, "gbk", true);

		StringMaxMatch stringMaxMatch = new StringMaxMatch();
		int window = 10;
		ArrayList<PatternString> patternStrings = new ArrayList<PatternString>();
		if (s1.length() <= s2.length()) {
			stringMaxMatch.matching(s1, s2, window, patternStrings);
		} else {
			stringMaxMatch.matching(s2, s1, window, patternStrings);
		}
	}

	public StringMaxMatch() {
	}

	/**
	 * 最大公共字串
	 * 
	 * */
	public String MaxSubstring(String s1, String s2) {
		char[] cs1 = s1.toCharArray();
		char[] cs2 = s2.toCharArray();
		int[][] partten = new int[cs1.length][cs2.length];
		for (int i = 0; i < partten.length; i++) {
			for (int j = 0; j < partten[0].length; j++) {
				if (cs1[i] == cs2[j]) {
					if (i > 0 && j > 0) {
						partten[i][j] = partten[i - 1][j - 1] + 1;
					} else {
						partten[i][j] = 1;
					}
				}
			}
		}
		int max = 0;
		int indexj = 0;
		for (int i = 0; i < partten.length; i++)// 找最大
		{
			for (int j = 0; j < partten[0].length; j++) {
				if (max < partten[i][j]) {
					max = partten[i][j];
					indexj = j;
				}
			}
		}
		if (max != 0) {
			return s2.substring(indexj - max + 1, indexj + 1);
		} else {
			return "";
		}
	}

	/**
	 * 求两个串匹配的最大长度 s1.length<=s2.length
	 * 
	 * @param s1
	 *            String one
	 * @param s2
	 *            String two
	 * */
	public int matching(String s1, String s2, int window, ArrayList<PatternString> patternStrings) {
		char[] cs1 = s1.toCharArray();
		char[] cs2 = s2.toCharArray();
		int[][] partten = new int[cs1.length][cs2.length];
		for (int i = 0; i < partten.length; i++) {
			for (int j = 0; j < partten[0].length; j++) {
				if (cs1[i] == cs2[j]) {
					if (i > 0 && j > 0) {
						partten[i][j] = partten[i - 1][j - 1] + 1;
					} else {
						partten[i][j] = 1;
					}
				}
			}
		}
	
		return getCommonWord(partten, window, s2, patternStrings);
	}

	/**
	 * 计算句子中最大匹配大小
	 * 
	 * */
	private int getCommonWord(int[][] partten, int window, String s2, ArrayList<PatternString> patternStrings) {

		int max = Integer.MAX_VALUE;
		int indexj = Integer.MAX_VALUE;
		int indexi = Integer.MAX_VALUE;

		int maxSim = 0;
		while (max >= window) {
			max = 0;
			indexj = 0;
			indexi = 0;
			for (int i = 0; i < partten.length; i++)// 找最大
			{
				for (int j = 0; j < partten[0].length; j++) {
					if (max < partten[i][j]) {
						max = partten[i][j];
						indexj = j;
						indexi = i;
					}
				}
			}

			int time = max;
			maxSim += max;

			if (max < window) {
				break;
			}

			String comString = delHeadTail(s2.substring(indexj - max + 1, indexj + 1));
			if (comString.length() > 0) {
				int index = isContain(patternStrings, comString);
				if (index == -1) {
					PatternString ps = new PatternString();
					ps.pattern = comString;
					
					//relativePositionPercentage 在文章中的相对位置
					ps.relativePositionPercentage = (indexj - max + 1.0) / s2.length();

					patternStrings.add(ps);

				} else {
					patternStrings.get(index).changeTime++;
				}
			}

			for (int i = indexi; i >= 0 && time > 0; i--, time--) {// 擦除所占据的行
				for (int j = 0; j < partten[0].length; j++) {
					partten[i][j] = 0;
				}
			}
			for (int i = indexj; i >= 0 && time > 0; i--, time--) {// 擦除所占据的列
				for (int j = 0; j < partten.length; j++) {
					partten[j][i] = 0;
				}
			}

		}

		return maxSim;
	}

	public static String delHeadTail(String s) {
		if (s.contains("<") && s.contains(">") && s.indexOf("<") < s.lastIndexOf(">") + 1) {
			// System.out.println(s.substring(s.indexOf("<"),
			// s.lastIndexOf(">")+1));
			return s.substring(s.indexOf("<"), s.lastIndexOf(">") + 1);
		} else if (s.contains("<") && s.contains(">") && s.indexOf("<") > s.lastIndexOf(">") + 1) {
			return s.substring(s.lastIndexOf(">"), s.length());
		} else if (s.contains(">") && !s.contains("<")) {
			// System.out.println(s.substring(0, s.lastIndexOf(">")+1));
			return s.substring(0, s.lastIndexOf(">") + 1);
		} else if (!s.contains(">") && s.contains("<")) {
			// System.out.println(s.substring(s.indexOf("<"), s.length()));
			return s.substring(s.indexOf("<"), s.length());
		} else if (!s.contains(">") && !s.contains("<")) {
			return "";
		}
		return s;
	}

	public int isContain(ArrayList<PatternString> patternStrings, String pat) {
		for (int i = 0; i < patternStrings.size(); i++) {
			if (patternStrings.get(i).pattern.equals(pat)) {
				return i;
			}
		}
		return -1;
	}

	/**
	 * 最大匹配数的总和
	 * 
	 * @param window
	 *            最小匹配窗口
	 * */
	public int MaxmatchNum(String s1, String s2, int window) {
		char[] cs1 = s1.toCharArray();
		char[] cs2 = s2.toCharArray();
		int[][] partten = new int[cs1.length][cs2.length];
		for (int i = 0; i < partten.length; i++) {
			for (int j = 0; j < partten[0].length; j++) {
				if (cs1[i] == cs2[j]) {
					if (i > 0 && j > 0) {
						partten[i][j] = partten[i - 1][j - 1] + 1;
					} else {
						partten[i][j] = 1;
					}
				}
			}
		}
		int max = Integer.MAX_VALUE;
		int maxNum = Integer.MAX_VALUE;
		int indexj = Integer.MAX_VALUE;
		int indexi = Integer.MAX_VALUE;

		int maxSim = 0;
		while (max >= window) {
			max = 0;
			indexj = 0;
			indexi = 0;
			for (int i = 0; i < partten.length; i++)// 找最大
			{
				for (int j = 0; j < partten[0].length; j++) {
					if (max < partten[i][j]) {
						max = partten[i][j];
						maxNum = j;
						indexj = j;
						indexi = i;
					}
				}
			}

			int time = max;
			maxSim += max;

			if (max < window) {
				break;
			}
			for (int i = indexi; i >= 0 && time > 0; i--, time--) {// 擦除所占据的行
				for (int j = 0; j < partten[0].length; j++) {
					partten[i][j] = 0;
				}
			}
			for (int i = indexj; i >= 0 && time > 0; i--, time--) {// 擦除所占据的列
				for (int j = 0; j < partten.length; j++) {
					partten[j][i] = 0;
				}
			}

		}
		// System.out.println("max match is : " + maxSim);
		cs1 = null;
		cs2 = null;
		partten = null;
		return maxSim;
	}

	/**
	 * 去掉字符串中乱码，标点符号等等
	 * 
	 * */
	private static String delSignForDir(String s) {
		char[] chars = s.toCharArray();
		for (int i = 0; i < chars.length; i++) {
			if (!((chars[i] >= 19968 && chars[i] <= 40869) || (chars[i] >= 97 && chars[i] <= 122) || (chars[i] >= 48 && chars[i] <= 59) || (chars[i] >= 65 && chars[i] <= 90))) {
				chars[i] = ' ';
			}
		}
		return String.valueOf(chars);
	}
}
