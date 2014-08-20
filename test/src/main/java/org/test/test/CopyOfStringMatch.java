package org.test.test;

/**
 * 
 * @author hwei.mike
 * */
public class CopyOfStringMatch {
	private int maxMatch;// 两个字符串匹配的最大长度的和 sum of max Match String

	private double simOf2Sentence; // 两个字符串的相似度
	private String s1; // 字符串1
	private String s2; // 字符串2

	/**
	 * @param sen1
	 *            字符串1
	 * @param sen2
	 *            字符串2
	 * @param window
	 *            最小匹配串口，一般2比较好，代表最小匹配单位是一个词
	 * */
	public CopyOfStringMatch(String sen1, String sen2, int window) {
		this.s1 = sen1;
		this.s2 = sen2;

		matching(window); // 计算相似度
	}

	/**
	 * 求两个串匹配的最大长度
	 * 
	 * */
	private void matching(int window) {
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
		// 输出矩阵，可分析
		/*
		 * for (int i = 0; i < partten.length; i++) { for (int j = 0; j <
		 * partten[0].length; j++) { System.out.print(partten[i][j]+" "); }
		 * System.out.println(); }
		 */
		maxMatch = getCommonWord(partten, window);

		// 制定相似度大小规则，相似度=最大匹配字串/两个字符串和的一半
		if (maxMatch < s1.length() && maxMatch < s2.length()) {
			simOf2Sentence = 2 * (maxMatch + 0.0) / (s1.length() + s2.length());

		} else {
			simOf2Sentence = 1.0d;
		}
		// 制定相似度大小规则，相似度=最大匹配字串/两个字符串和的一半
		// 如果一个串包含另一个串，则相似度为1（可选择）
		/*
		 * if(maxMatch<s1.length() && maxMatch<s2.length()){ simOf2Sentence =
		 * 2*(maxMatch+0.0)/(s1.length()+s2.length());
		 * 
		 * }else { simOf2Sentence = 1.0d; }
		 */
	}

	/**
	 * 计算句子中最大匹配大小
	 * 
	 * */
	private int getCommonWord(int[][] partten, int window) {

		int max = Integer.MAX_VALUE;
		int maxNum = Integer.MAX_VALUE;
		int indexj = Integer.MAX_VALUE;
		int indexi = Integer.MAX_VALUE;

		int maxSim = 0;
		while (max >= window) {
			max = 0;
			indexj = 0;
			indexi = 0;
			for (int i = 0; i < partten.length; i++) {
				for (int j = 0; j < partten[0].length; j++) {
					if (max < partten[i][j]) {
						max = partten[i][j];
						maxNum = j;
						indexj = j;
						indexi = i;
					}
				}
			}

			/*
			 * System.out.println(indexi+" "+indexj+" "+max+"　"+maxNum);
			 * System.out.println(s2.substring(indexj-max+1, indexj+1));
			 */

			int time = max;
			maxSim += max;

			for (int i = indexi; i >= 0 && time > 0; i--, time--) {
				for (int j = 0; j < partten[0].length; j++) {
					partten[i][j] = 0;
				}
			}
			for (int i = indexj; i >= 0 && time > 0; i--, time--) {
				for (int j = 0; j < partten.length; j++) {
					partten[j][i] = 0;
				}
			}

		}
		System.out.println("max match is : " + maxSim);
		return maxSim;
	}

	public int getMaxMatch() {
		return maxMatch;
	}

	public double getSimOf2Sentence() {
		return simOf2Sentence;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		String s1 = "经济有望继续上升：房地产市场有望企稳。5月房屋新开工面积下降7.9%，收窄3.6和4.9个百分点；";
		String s2 = "经济有望继续上升asdf：房地产市场有望asdfsa企稳。dssfsadfsadfsad5月房屋新开asfsadf工面积下降7.9%，收窄3.6和asdfsadf4.9个百分点；";

		String ss1 = delSignForDir(s1).replace(" ", "");
		String ss2 = delSignForDir(s2).replace(" ", "");
		System.out.println(delSignForDir(s1).replace(" ", ""));
		System.out.println(delSignForDir(s2).replace(" ", ""));

		CopyOfStringMatch lcs = new CopyOfStringMatch(ss1, ss2, 2);
		System.out.println(lcs.getMaxMatch());
		System.out.println(lcs.getSimOf2Sentence());

		System.out.println(ss1);
		System.out.println(ss2);
	}

	/**
	 * 去掉字符串中乱码，标点符号等等
	 * 
	 * */
	private static String delSignForDir(String s) {
		char[] chars = s.toCharArray();
		for (int i = 0; i < chars.length; i++) {
			if (!((chars[i] >= 19968 && chars[i] <= 40869) || (chars[i] >= 97 && chars[i] <= 122)
					|| (chars[i] >= 48 && chars[i] <= 59) || (chars[i] >= 65 && chars[i] <= 90))) {
				chars[i] = ' ';
			}
		}
		return String.valueOf(chars);
	}
}
