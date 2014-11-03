package org.test.model;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.test.bean.PatternString;
import org.test.utils.IOUtils;
import org.test.utils.StringMaxMatch;

public class FindContent {
	static IOUtils rw_Utils = new IOUtils();
	static StringMaxMatch stringMaxMatch = new StringMaxMatch();
	static String encoding = "UTF-8";
	static Logger logger = Logger.getLogger("s");

	public ArrayList<PatternString> patternTrain(String path, int TRAIN_NUM, int window) {
		logger.info(path + " has start to train ! ");

		ArrayList<PatternString> patternStringsCommon = new ArrayList<PatternString>();
		File[] files = new File(path + "/news/").listFiles();

		if (files == null) {
			return null;
		}

		// 训练10篇文章
		if (files.length < 10) {
			TRAIN_NUM = files.length;
		} else {
			TRAIN_NUM = 10;
		}

		
		for (int m = 0; m < TRAIN_NUM && m < files.length; m++) {// 训练文件数控制
			System.out.println(files[m].length());
			if (files[m].length() > 45000) {
				TRAIN_NUM++;
				continue;
			}
			for (int i = m + 1; i < TRAIN_NUM && i < files.length; i++) {
				if (files[i].length() > 45000) {
					TRAIN_NUM++;
					continue;
				}
				
				System.out.println("traning " +  files[m].length()  +" , "+ files[i].length());
				String s1 = rw_Utils.Read(files[m], encoding);
				String s2 = rw_Utils.Read(files[i], encoding);

				// int window = 10;//windows
				// 提取待选择模板
				ArrayList<PatternString> patternStrings = new ArrayList<PatternString>();
				if (s1.length() <= s2.length()) { // 长度较小的文章，放在前面。
					stringMaxMatch.matching(s1, s2, window, patternStrings);
				} else {
					stringMaxMatch.matching(s2, s1, window, patternStrings);
				}
				// 模板整合
				if (i == 1) {
					patternStringsCommon = (ArrayList<PatternString>) patternStrings.clone();//
				} else {
					SimListAnalysis(patternStringsCommon, patternStrings, path + "/temp/" + "pattern.txt");// out1
				}
			}
		}
		logger.info(path + " pattern train end ! start to get patternCommonString!");

		getPatternCommon(patternStringsCommon, path);// out2
		logger.info(path + " get patternCommonString end !");
		// }
		return patternStringsCommon;
	}

	/**
	 * 比较，更新公共模板(模板整合)
	 * 
	 * @param patternStringsCommon
	 *            模板组
	 * @param patternStrings
	 *            待整合的模板
	 * @param path
	 *            模板输出路径
	 * 
	 * */
	public void SimListAnalysis(ArrayList<PatternString> patternStringsCommon, ArrayList<PatternString> patternStrings, String path) {
		for (int i = 0; i < patternStrings.size(); i++) {
			int index = isContain(patternStringsCommon, patternStrings.get(i).pattern);
			if (index != -1) {
				patternStringsCommon.get(index).changeTime++;
			} else {
				patternStringsCommon.add(patternStrings.get(i));
			}
		}

		String text = "";
		for (int i = 0; i < patternStrings.size(); i++) {
			text += patternStrings.get(i).changeTime + "\t" + patternStrings.get(i).pattern + "\t" + patternStrings.get(i).relativePositionPercentage + "\n";

		}
		rw_Utils.Write(path, text, encoding, true);
	}

	/**
	 * 对模板进行的优化措施
	 * */
	public void getPatternCommon(ArrayList<PatternString> patternStringsCommon, String path) {
		Collections.sort(patternStringsCommon, new Comparator<PatternString>() {// sort
					@Override
					public int compare(PatternString o1, PatternString o2) {
						if (o1.relativePositionPercentage > o2.relativePositionPercentage)
							return 1;
						else if (o1.relativePositionPercentage < o2.relativePositionPercentage) {
							return -1;
						}
						return 0;
					}
				});

		for (int i = 0; i < patternStringsCommon.size(); i++) {// 出现次数较小的去除，这里阈值10.。阈值可以根据统计结果去断定
			// pattern.length > window
			if (patternStringsCommon.get(i).changeTime < 10 || patternStringsCommon.get(i).pattern.length() < 30) {
				patternStringsCommon.remove(i);
				i--;
			}
		}

		// 写入磁盘（未整合的模板组）
		String text0 = "";
		for (int i = 0; i < patternStringsCommon.size(); i++) {
			// if(){
			text0 += patternStringsCommon.get(i).changeTime + "\t" + patternStringsCommon.get(i).pattern + "\t" + patternStringsCommon.get(i).relativePositionPercentage + "\n";
			// }
		}
		// if(!new File(path+"PatternStringCommon0.txt").exists()){
		rw_Utils.Write(path + "/temp/" + "PatternStringCommon0.txt", text0, encoding, false);
		logger.info(path + " Write PatternStringCommon0.txt succeed !");

		// }
		// 对模板进行整合
		for (int i = 0; i < patternStringsCommon.size(); i++) {// 去除重复的模板
			double MAX = 0.5;// 相似度阈值。需要经验设定
			int index = 0;
			for (int j = i + 1; j < patternStringsCommon.size(); j++) {
				String s1 = patternStringsCommon.get(i).pattern;
				String s2 = patternStringsCommon.get(j).pattern;
				// 计算模板之间相似度，去除相似度一样的模板
				double sim = StringCompareMaxSimStr2(s1, s2);// StringCompareMaxSim(s1,
																// s2);
				if (sim > MAX) {
					MAX = sim;
					index = j;
					break;
				}
			}

			// 模板删除(待优化)两个相似的模板，一定要删除长的那个么。？。是否要考虑一下模板出现的次数呢？
			if (MAX == 1.0) {// 相同模板删除，包含模板，删除短的模板
				int a = patternStringsCommon.get(i).pattern.length() < patternStringsCommon.get(index).pattern.length() ? i : index;
				logger.info(path + " Del from PatternCommonString0 ");
				logger.info(path + " Del from PatternCommonString0 " + patternStringsCommon.get(a).pattern + " SIM: " + MAX);
				patternStringsCommon.remove(a);
				i--;
			} else if (MAX > 0.5 && MAX < 1.0) {// 相似模板删除，删除长的模板
				int a = patternStringsCommon.get(i).pattern.length() < patternStringsCommon.get(index).pattern.length() ? i : index;
				logger.info(path + " Del from PatternCommonString0 ");
				logger.info(path + " Del from PatternCommonString0 " + patternStringsCommon.get(a).pattern + " SIM: " + MAX);
				patternStringsCommon.remove(a);
				i--;
			}
		}

		String text = "";
		for (int i = 0; i < patternStringsCommon.size(); i++) {
			text += patternStringsCommon.get(i).changeTime + "\t" + patternStringsCommon.get(i).pattern + "\t" + patternStringsCommon.get(i).relativePositionPercentage + "\n";
		}
		// 生成优化后的模板 PatternStringCommon.txt
		rw_Utils.Write(path + "/temp/" + "PatternStringCommon.txt", text, encoding, false);
		logger.info(path + " Write PatternStringCommon.txt succeed !");

	}

	/**
	 * 两个串的相似度
	 * 
	 * */
	/*
	 * public double StringCompareMaxSim(String s1 , String s2) {
	 * if(s1.contains(s2)||s2.contains(s1)) return 1.0;
	 * 
	 * int maxNum = 0; if(s1.length() < s2.length()){ maxNum =
	 * stringMaxMatch.MaxmatchNum(s1, s2, 10); }else { maxNum =
	 * stringMaxMatch.MaxmatchNum(s2, s1, 10); } return
	 * (maxNum*2)/(s1.length()+s2.length()+0.0); }
	 */

	/**
	 * 两个串的相似度 2 _ new
	 * 
	 * */
	public double StringCompareMaxSimStr2(String s1, String s2) {
		if (s1.contains(s2) || s2.contains(s1))
			return 1.0;
		int maxNum = 0;
		if (s1.length() < s2.length()) {
			maxNum = stringMaxMatch.MaxmatchNum(s1, s2, 30);
			return (maxNum) / (s1.length() + 0.0);
		} else {
			maxNum = stringMaxMatch.MaxmatchNum(s2, s1, 30);
			return (maxNum) / (s2.length() + 0.0);
		}
		// return (maxNum*2)/(s1.length()+s2.length()+0.0);
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
	 * 去掉字符串中乱码，标点符号等等
	 * 
	 * */
	public static String delSignForDir(String s) {
		char[] chars = s.replaceAll("<.*?>", "").toCharArray();
		for (int i = 0; i < chars.length; i++) {
			if (!((chars[i] >= 19968 && chars[i] <= 40869) || (chars[i] >= 97 && chars[i] <= 122) || (chars[i] >= 48 && chars[i] <= 59) || (chars[i] >= 65 && chars[i] <= 90))) {
				chars[i] = ' ';
			}
		}
		return String.valueOf(chars).replace(" ", "");
	}

	public static String delHtmlTag(String s) {
		return s.replaceAll("<.*?>", "").replace(" ", "");
	}
	
	public static String delSameHtmlTag(String s) {
		String re = "</?[h1|div|span|h2|h3].*?>";
		Pattern pAlink = Pattern.compile(re, Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.MULTILINE);
		Matcher matcher = pAlink.matcher(s);
		String filterResult = matcher.replaceAll(" ");
		return filterResult;
	}

	public static String getHtmlTag(String s) {
		return s.replaceAll(">.*?</", "></").replace(" ", "");
	}
}
