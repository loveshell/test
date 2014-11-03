package org.test.model;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.test.bean.PatternIndex;
import org.test.bean.PatternString;
import org.test.utils.IOUtils;
import org.test.utils.StringMaxMatch;

//java -jar getNewContent.jar /data/hwei/newsFetcher/news/ /data/hwei/newsFetcher/newsout/ GBK
public class GetContent {
	static IOUtils rw_Utils = new IOUtils();
	static StringMaxMatch stringMaxMatch = new StringMaxMatch();
	static FindContent findContent = new FindContent();
	static String encoding = "UTF-8";
	static Logger logger = Logger.getLogger("s");

	public void patternTest(String path, int SELECT_NUM, int WINDOW) {
		String filePathtest = path + "/test/";
		File filePathDirtest = new File(filePathtest);
		filePathDirtest.mkdirs();

		String filePathtemp = path+ "/temp/";
		File filePathDirtemp = new File(filePathtemp);
		filePathDirtemp.mkdirs();
		
		ArrayList<PatternString> patternStringsCommon = findContent.patternTrain(path, 10, WINDOW);

		if (patternStringsCommon == null) {
			return;
		}
		// 模板选择
		logger.info(path + " patternCommonString train end ! Start to select pattern !");

		// 模板测试
		File f = new File(path + "/news/");// 网页路径
		File[] files = f.listFiles();
		
		ArrayList<PatternIndex> patternIndexs = new ArrayList<PatternIndex>();
		// 模板回测---初始化patternIndexs

		if (files.length < 10) {
			SELECT_NUM = files.length;
		} else {
			SELECT_NUM = 10;
		}

		for (int i = 0; i < SELECT_NUM; i++) {
			String s = rw_Utils.Read(files[i], encoding);
			String title = files[i].getName();
			System.out.println("title : " + title);
			getContent(title, s, patternStringsCommon, patternIndexs, path + "/temp/Content.txt");// 寻找正文模板
		}

		logger.info(path + " patternIndexs init end! Start to Select Pattern !");
		contentConfirmByFile(patternIndexs, patternStringsCommon, path);// 确定模板

	}

	public int isContain(ArrayList<PatternIndex> patternIndexs, int start, int end) {
		for (int i = 0; i < patternIndexs.size(); i++) {
			if (patternIndexs.get(i).start == start && patternIndexs.get(i).end == end) {
				return i;
			}
		}
		return -1;
	}

	/**
	 * 模板回测，寻找正文模板
	 * 
	 * */
	public void getContent(String title, String htmlText, ArrayList<PatternString> patternStringsCommon, ArrayList<PatternIndex> patternIndexs, String path) {
		ArrayList<String> Content = new ArrayList<String>();
		System.out.println(patternStringsCommon.size());
		for (int i = 0; i < patternStringsCommon.size() - 1; i++) {
			int index = 0;
			int end = 0;
			for (int j = i + 1; j < patternStringsCommon.size(); j++) {
				if (htmlText.contains(patternStringsCommon.get(i).pattern) && htmlText.contains(patternStringsCommon.get(j).pattern)) {
					index = htmlText.indexOf(patternStringsCommon.get(i).pattern);
					end = htmlText.indexOf(patternStringsCommon.get(j).pattern);
				}
				if (index > 0 && end > 0 && index + patternStringsCommon.get(i).pattern.length() < end) {
					String temp = FindContent.delHtmlTag(htmlText.substring(index + patternStringsCommon.get(i).pattern.length(), end));

					// 取到测试模板内容,比较与标题的相似度
					int likeTitle = 0;
					if (title.length() <= temp.length()) { // 长度较小的文章，放在前面。
						likeTitle = stringMaxMatch.matching(title, temp, 2, new ArrayList<PatternString>());
					} else {
						likeTitle = stringMaxMatch.matching(temp, title, 2, new ArrayList<PatternString>());
					}

					System.out.println("likeTitle : " + likeTitle);

					if (!Content.contains(temp)) {
						int PindexTemp = isContain(patternIndexs, i, j);
						if (PindexTemp != -1) {
							patternIndexs.get(PindexTemp).count++;
							patternIndexs.get(PindexTemp).contentLength += temp.length();
							patternIndexs.get(PindexTemp).likeTile += likeTitle; // 相加等到和
						} else {
							PatternIndex patternIndex = new PatternIndex();
							patternIndex.start = i;
							patternIndex.end = j;
							patternIndex.count++;
							patternIndex.contentLength = temp.length();
							patternIndex.likeTile = likeTitle;
							patternIndexs.add(patternIndex);
						}
						i = j - 1;
						Content.add(temp);
						break;
					}
				} else {
					break;
				}
			}
		}
		String text = "";
		for (String string : Content) {
			text += string + "\n";
		}
		text += "-----------------------------------------------------------------";
		rw_Utils.Write(path, text, encoding, false);
	}

	/**
	 * 针对同一站点提取出的模板进行筛选，提取出最能作为正文的一组模板
	 * 
	 * @param patternIndexs
	 *            模板对应信息存储链表
	 * @param patternStringsCommon
	 *            待整合的模板组
	 * @param pathFileOut
	 *            测试结果输出路径
	 * @param testPathIN
	 *            测试语料输入路径
	 * 
	 * */
	public void contentConfirmByFile(ArrayList<PatternIndex> patternIndexs, ArrayList<PatternString> patternStringsCommon, String path) {
		System.out.println("patternIndexs.size() : " + patternIndexs.size());
		if (patternIndexs.size() == 0) {
			return;
		}

		for (PatternIndex patternIndex : patternIndexs) {
			patternIndex.avgLikeTile = patternIndex.likeTile / (patternIndex.count + 0.0);
		}

		Collections.sort(patternIndexs, new Comparator<PatternIndex>() {// sort
					@Override
					public int compare(PatternIndex arg0, PatternIndex arg1) {
						if (arg0.avgLikeTile < arg1.avgLikeTile) {
							return 1;
						} else if (arg0.avgLikeTile > arg1.avgLikeTile) {
							return -1;
						}
						return 0;
					}
				});

		// get best pattern
		patternIndexs.get(0).isArctile = true;
		int s = patternIndexs.get(0).start;
		int e = patternIndexs.get(0).end;

		String text = "";
		for (PatternIndex patternIndex : patternIndexs) {
			text += patternIndex.avgLikeTile + "\t" + patternIndex.isArctile + "\t" + patternIndex.count + "\t" + patternIndex.start + "\t" + patternIndex.end + "\n";
		}
		rw_Utils.Write(path + "/temp/" + "patternIndex.txt", text, encoding, false);
		text = null;

		logger.info(path + " Select Pattern end ! Write PatternIndex.txt succeed ! ");
		logger.info(path + " Start to test ! ");

		getContentByFile(patternStringsCommon.get(s).pattern, patternStringsCommon.get(e).pattern, path);
		// getContentByFileAll(patternStringsCommon, patternIndexs, path);
		// getContentByChangePattern2(patternStringsCommon, patternIndexs, path,
		// 0);
	}

	/**
	 * 针对模板进行网页抽取测试
	 * 
	 * @param patternStringsCommon
	 *            模板组
	 * @param start
	 *            正文模板索引
	 * @param end
	 *            正文模板索引
	 * @param pathFileOut
	 *            正文输出路径
	 * @param testPath
	 *            测试文本输入路径
	 * 
	 * */
	/*
	 * public void getContentTest(ArrayList<PatternString> patternStringsCommon
	 * , int start ,int end , String testPath,String pathFileOut ) { String con
	 * = "";
	 * 
	 * File[] files = new File(testPath).listFiles(); for (int i = 0; i <
	 * files.length; i++) { String text = rw_Utils.Read(files[i], encoding); int
	 * index1 = text.indexOf(patternStringsCommon.get(start).pattern); int
	 * index2 = text.indexOf(patternStringsCommon.get(end).pattern); if(index1>0
	 * && index2>0 && index1 != -1 && index2 != -1 &&
	 * index1+patternStringsCommon.get(start).pattern.length()<index2 ){ String
	 * temp =
	 * FindContent.delHtmlTag(text.substring(index1+patternStringsCommon.get
	 * (start).pattern.length(), index2)); con += temp
	 * +"\n--------------------------------------------------------\n"; } }
	 * 
	 * rw_Utils.Write(pathFileOut, con, encoding, false); }
	 */

	/**
	 * 针对模板进行网页抽取测试 <h1>此算法为强测试，只拿一组模板回测</h1>
	 * 
	 * @param patternStringsCommon
	 *            模板组
	 * @param start
	 *            正文模板索引
	 * @param end
	 *            正文模板索引
	 * @param pathFileOut
	 *            正文输出路径
	 * @param testPath
	 *            测试文本输入路径
	 * 
	 * */
	public void getContentByFile(String start, String end, String path) {
		// String con = "";
		File[] files = new File(path + "/news/").listFiles();
		for (int i = 0; i < files.length; i++) {
			String text = rw_Utils.Read(files[i], encoding);
			int index1 = text.indexOf(start);
			int index2 = text.indexOf(end);
			System.out.println("-------------------------------------------------------------");
			System.out.println("index1 " + index1 + ", index2 " + index2);
			
			String temp = "";
			if (index1 > 0 && index2 > 0 && index1 + start.length() < index2) {
				// temp = FindContent.delHtmlTag(text.substring(index1 +
				// start.length(), index2)).replace("&nbsp;", "");

				temp = text.substring(index1 + start.length(), index2);
				System.out.println("files[i].getName() " + files[i].getName() + " temp " + temp.length());
				temp = FindContent.delSameHtmlTag(temp);
				rw_Utils.Write(path + "/test/" + files[i].getName(), temp, encoding, false);
			} else {
				String sTemp = "";
				if (index1 < 0) {
					for (int si = 0; si < start.length(); si++) {
						sTemp = start.substring(si);
						if (sTemp.length() < 6) { // 小于6的长度直接舍去
							break;
						}
						index1 = text.indexOf(sTemp);
						if (index1 > 0) {
							System.out.println("start : " + sTemp);
							break;
						}
					}

					// 如果没找到，再反过来找
					if (index1 < 0) {
						for (int si = start.length() - 1; si > 0; si--) {
							sTemp = start.substring(0, si);

							if (sTemp.substring(sTemp.length() - 1).equals("<")) {
								sTemp = sTemp.substring(0, sTemp.length() - 1);
								index1 = text.indexOf(sTemp);
								if (index1 > 0) {
									System.out.println("start : " + sTemp);
									break;
								}
							} else {
								index1 = text.indexOf(sTemp);
								if (index1 > 0) {
									System.out.println("start : " + sTemp);
									break;
								}
							}

						}
					}
				} else {
					sTemp = start;
				}

				String eTemp = "";
				if (index2 < 0) {
					String startStr = text.substring(index1 + sTemp.length());
					for (int ei = end.length() - 1; ei > 0; ei--) {
						eTemp = end.substring(0, ei);
						index2 = startStr.indexOf(eTemp);
						if (index2 > 0 ) {
							System.out.println("end : " + eTemp);
							System.out.println("index2  first " + index2);
							index2 = getEndIndex(text, eTemp, index2);
							System.out.println("index2  last " + index2);
							break;
						}
						index2 += index1 + sTemp.length();
					}
				}
				System.out.println("index1 : " + index1 + " , sTemp :" +  sTemp.length() + " , index2 :" + index2);
				if ((index1 + sTemp.length()) < index2) {
					temp = text.substring(index1 + sTemp.length(), index2);
				}
				System.out.println("temp : " + temp.length());
				if (temp.indexOf("-->") != -1) {
					temp = temp.substring(temp.indexOf("-->") + "-->".length());
				}

				temp = FindContent.delSameHtmlTag(temp);

				System.out.println("files[i].getName() " + files[i].getName());
				System.out.println("-------------------------------------------------------------");
				rw_Utils.Write(path + "/test/" + files[i].getName(), temp, encoding, false);

			}
		}
		logger.info(path + " Test end ! ");
	}

	public int getEndIndex(String text, String end, int index2) {
		int result = text.indexOf(end, index2+end.length());
		if (result == -1) {
			return index2;
		}
		return getEndIndex(text, end, result);
	}

	/**
	 * 针对模板进行网页抽取测试 <h1>此算法为弱测试，只拿多组模板回测，知道返回正文</h1>
	 * 
	 * @param patternStringsCommon
	 *            模板组
	 * @param start
	 *            正文模板索引
	 * @param end
	 *            正文模板索引
	 * @param pathFileOut
	 *            正文输出路径
	 * @param testPath
	 *            测试文本输入路径
	 * 
	 * */
	public void getContentByFileAll(ArrayList<PatternString> patternStringsCommon, ArrayList<PatternIndex> patternIndexs, String path) {
		// String con = "";
		File[] files = new File(path + "/news/").listFiles();
		for (int i = 0; i < files.length; i++) {
			String text = rw_Utils.Read(files[i], encoding);
			for (int j = 0; j < patternIndexs.size(); j++) {
				if (patternIndexs.get(j).count == 0) {
					break;
				}
				String start = patternStringsCommon.get(patternIndexs.get(j).start).pattern;
				String end = patternStringsCommon.get(patternIndexs.get(j).end).pattern;
				int index1 = text.indexOf(start);
				int index2 = text.indexOf(end);
				String temp = "";
				if (index1 > 0 && index2 > 0 && index1 != -1 && index2 != -1 && index1 + start.length() < index2) {
					temp = FindContent.delHtmlTag(text.substring(index1 + start.length(), index2)).replace("&nbsp;", "");
					// con += temp
					// +"\n--------------------------------------------------------\n";
					rw_Utils.Write(path + "/test/" + files[i].getName(), temp, encoding, false);
					temp = null;
					start = null;
					end = null;
					break;
				}
				start = null;
				end = null;
			}
		}
		logger.info(path + " Test end ! ");
	}

	/**
	 * 针对模板进行网页抽取测试
	 * 
	 * 
	 * (模糊匹配)
	 * 
	 * 
	 * <h1>此算法为强测试，只拿一组模板回测</h1>
	 * 
	 * @param patternStringsCommon
	 *            模板组
	 * @param start
	 *            正文模板索引
	 * @param end
	 *            正文模板索引
	 * @param pathFileOut
	 *            正文输出路径
	 * @param testPath
	 *            测试文本输入路径
	 * 
	 * */
	public void getContentByChangePattern2(ArrayList<PatternString> patternStringsCommon, ArrayList<PatternIndex> patternIndexs, String path, int cir) {
		// String con = "";
		cir++;

		int all = 0;
		int count = 0;
		File[] files = new File(path + "/news/").listFiles();
		int pIs = patternIndexs.get(0).start;
		int pIe = patternIndexs.get(0).end;
		String start = "";
		String end = "";
		if (pIs - cir > 0 && pIe + cir < patternStringsCommon.size()) {
			start = patternStringsCommon.get(pIs - cir).pattern;
			end = patternStringsCommon.get(pIe + cir).pattern;
		} else if (pIs - cir == 0 && pIe + cir < patternStringsCommon.size()) {
			start = patternStringsCommon.get(pIs).pattern;
			end = patternStringsCommon.get(pIe + cir).pattern;
		} else if (pIs - cir > 0 && pIe + cir < patternStringsCommon.size()) {
			start = patternStringsCommon.get(pIs - cir).pattern;
			end = patternStringsCommon.get(pIe).pattern;
		} else if (pIs - cir < 0 && pIe + cir == patternStringsCommon.size()) {
			return;
		}

		ArrayList<String> StartLists = new ArrayList<String>();
		ArrayList<String> EndLists = new ArrayList<String>();
		Pattern p = Pattern.compile("<.*?>");
		MatcherTag(p, start, StartLists);
		MatcherTag(p, end, EndLists);

		for (int i = 0; i < files.length; i++) {
			String text = rw_Utils.Read(files[i], encoding);
			int index1 = text.indexOf(start);
			int index2 = text.indexOf(end);
			String temp = "";
			if (index1 > 0 && index2 > 0 && index1 != -1 && index2 != -1 && index1 + start.length() < index2) {
				temp = FindContent.delHtmlTag(text.substring(index1 + start.length(), index2)).replace("&nbsp;", "");
				// con += temp
				// +"\n--------------------------------------------------------\n";
			}
			if (temp.length() == 0) {
				int index_1 = text.indexOf(start.substring(start.length() / 3, start.length() / 3 * 2));
				int index_2 = text.indexOf(end.substring(end.length() / 3, end.length() / 3 * 2));
				if (index_1 > 0 && index_2 > 0 && index_1 != -1 && index_2 != -1 && index_1 + start.length() < index_2) {
					temp = FindContent.delHtmlTag(text.substring(index_1 + start.length() / 3 * 2, index_2 + end.length() / 3)).replace("&nbsp;", "");
					// con += temp
					// +"\n--------------------------------------------------------\n";
				}
			}
			if (temp.length() == 0) {
				String subStart = "";
				int subStartIndex = -1;
				for (int m = 0; m < StartLists.size(); m++) {
					subStart += StartLists.get(StartLists.size() - m - 1);
					int tempSub = text.indexOf(subStart);
					if (subStart.length() == 0) {
						continue;
					}
					if (tempSub != -1) {
						subStartIndex = tempSub;
						// break;
					} else {
						break;
					}
				}

				String subEnd = "";
				int subEndIndex = -1;
				for (int m = 0; m < EndLists.size(); m++) {
					subEnd += EndLists.get(m);
					int tempSub = text.indexOf(subEnd);
					if (subEnd.length() == 0) {
						continue;
					}
					if (tempSub != -1) {
						subEndIndex = tempSub;
						// break;
					} else {
						break;
					}

				}
				if (subStartIndex > 0 && subEndIndex > 0 && subStartIndex + subStart.length() < subEndIndex) {
					temp = FindContent.delHtmlTag(text.substring(subStartIndex, subEndIndex)).replace("&nbsp;", "");
					// con += temp
					// +"\n--------------------------------------------------------\n";
				}

			}
			if (temp.length() == 0) {

				logger.info(" File fail : " + files[i].getName());

				getContentByChangePattern2(patternStringsCommon, patternIndexs, path, cir);

				count++;
			} else {
				rw_Utils.Write(path + "/test/" + files[i].getName(), temp, encoding, false);
				return;
			}
			all++;
			temp = null;
		}
		logger.info(path + " Test end ! ");
		logger.info(" fail num is " + count + "  ->  Err is " + count / (double) all);
	}

	public void MatcherTag(Pattern pattern, String tags, ArrayList<String> tagLists) {
		Matcher matcher = pattern.matcher(tags);
		while (matcher.find()) {
			// System.out.println(matcher.group());
			tagLists.add(matcher.group());
		}
	}
}
