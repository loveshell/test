package org.test.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.lang.CharUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * // 过滤规则 ad 和 *ad* 是一样。 // $ 指明后面的是过滤类型，比如是是image还是script等 //
 * 使用管线符号（|）来表示地址的最前端或最末端。 // 以@@开始，则是白名单， // 以||开始则是不匹配协议名的过滤，并去掉|| // ！ 开始表示注释
 * // * 通配符，匹配任何字符串 // 标记分隔符 　^　分隔符可以是除了字母、数字或者 _ - . % //
 * 之外的任何字符。http://example.com:8000/foo.bar?a=12 这个地址可以通过这些过滤规则过滤 //
 * ^example.com^ 或 ^foo.bar^ 。 // 元素隐藏 这里的 ## 表明这是一条元素隐藏规则
 * 
 * @author xxhuang
 * 
 */
public class AdblockUtil {
	private static final Logger LOG = LoggerFactory.getLogger(AdblockUtil.class);

	// url中含有这些string
	private static Map<String, String> urlIncludeStrs = new HashMap();
	// url 以这些开始
	private static Map<String, String> urlStartStrs = new HashMap();
	// url 以这些结束
	// private static Map urlEndStrs = new HashMap();
	// 只按主机名过滤
	public static Map hosts = new HashMap();
	// 主机名+path
	public static Map hostUrls = new HashMap();
	public static Map paths = new HashMap();

	public static void loadADRecords(Configuration conf) {
		BufferedReader br = null;
		// BufferedWriter writer = null;
		long cnt = 0l;
		try {
			Reader reader = null;
			if (conf != null)
				reader = conf.getConfResourceAsReader("easylistchina+easylist.txt");
			else
				reader = new FileReader("d:/easylistchina+easylist.txt");
			br = new BufferedReader(reader);
			// writer = new BufferedWriter(new FileWriter("d:/adlist.txt"));

			String line = null;
			while ((line = br.readLine()) != null) {
				if (line.startsWith("[") || line.startsWith("!") || line.startsWith("@@") || line.endsWith("|")
						|| hasUseless(line))
					continue;
				if (line.indexOf("#") > -1)
					continue;
				if (line.indexOf("^") > -1) {
					if (!(line.startsWith("^") || line.endsWith("^")))
						continue;
				} // --------------up-filter

				cnt++;
				if (line.startsWith("||")) // 无用
					line = line.substring(2);
				int idx = -1;
				if ((idx = line.indexOf("$")) > -1)
					line = line.substring(0, idx);
				line = line.replaceAll("http://", "").replaceAll("http:", "").replaceAll("://", "");
				// --------------
				if (line.startsWith("|")) {
					line = line.substring(1);
					if (line.startsWith("*")) {
						line = line.substring(1);
						urlIncludeStrs.put(line, null);
					} else {
						urlStartStrs.put(line, null);
					}
				} else if (line.indexOf("^") > -1) {
					if (line.startsWith("^") && line.endsWith("^")) {
						urlIncludeStrs.put(line.substring(1, line.length() - 1), null);
					} else if (line.startsWith("^")) {
						// urlIncludeStrs.put(line.substring(1), null);
					} else {
						line = line.substring(0, line.length() - 1);
						urlIncludeStrs.put(line, null);
					}
				} else {
					urlIncludeStrs.put(line, null);
				}
			}

			processRules(urlStartStrs);
			processRules(urlIncludeStrs);

			// printResults(writer);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				br.close();
				// writer.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		System.out.println("hosts=" + hosts.size());
		System.out.println("hostUrls=" + hostUrls.size());
		System.out.println("paths=" + paths.size());
	}

	public static boolean isAD(String url, String host) {
		String smallHost = host.substring(host.indexOf(".") + 1);
		if (hosts.containsKey(host) || hosts.containsKey(smallHost)) {
			// System.out.println("type host=" + smallHost);
			return true;
		}
		if (hostUrls.containsKey(host)) {
			if (url.indexOf(host + hostUrls.get(host)) > -1) {
				// System.out.println("type url=" + host);
				return true;
			}
		} else if (hostUrls.containsKey(smallHost)) {
			if (url.indexOf(smallHost + hostUrls.get(smallHost)) > -1) {
				// System.out.println("type url=" + smallHost);
				return true;
			}
		}
		for (Iterator iterator = paths.keySet().iterator(); iterator.hasNext();) {
			String path = (String) iterator.next();
			if (url.indexOf(path) > -1) {
				// System.out.println("type path=" + path);
				return true;
			}
		}

		return false;
	}

	private static boolean hasUseless(String line) {
		return (line.endsWith(".js") || line.endsWith(".png") || line.endsWith(".jpg") || line.endsWith(".gif")
				|| line.endsWith(".swf") || line.indexOf("script") > -1 || line.indexOf("image") > -1
				|| line.indexOf("object") > -1 || line.indexOf("stylesheet") > -1
				|| line.indexOf("object-subrequest") > -1 || line.indexOf("domain=") > -1 || line.indexOf("https") > -1);
	}

	private static void processRules(Map<String, String> map) {
		for (Iterator<String> iterator = map.keySet().iterator(); iterator.hasNext();) {
			String line = iterator.next().toLowerCase();
			if (hasUseless(line))
				continue;

			int idx = -1;
			if ((idx = line.indexOf("*")) > -1) {
				if (idx != line.length() - 1) {// 中间
					continue;
				}
				line = line.substring(0, line.length() - 1);
			}
			if (line.length() <= 5) {
				// System.out.println(line);
				continue;
			}

			if ((idx = line.indexOf("/")) > -1) {
				String host = line.substring(0, idx);
				String path = line.substring(idx);
				if (line.startsWith("/")) {
					paths.put(path, null);
				} else {
					if (isLeagleHost(host))
						hostUrls.put(host, path);
					else
						paths.put(line, null);
				}
			} else {
				if (isLeagleHost(line))
					hosts.put(line, null);
				else
					paths.put(line, null);
			}
		}
	}

	private static boolean isLeagleHost(String line) {
		if (StringUtils.isEmpty(line)) {
			return false;
		}
		if (line.startsWith(".") || line.endsWith(".") || line.startsWith("-") || line.endsWith("-"))
			return false;
		int idx = -1;
		if ((idx = line.lastIndexOf(".")) == -1)
			return false;
		String tmp = line.substring(idx + 1);
		if (!StringUtils.isAlpha(tmp))
			return false;

		int sz = line.length();
		for (int i = 0; i < sz; i++) {
			if (!(CharUtils.isAsciiAlphanumeric(line.charAt(i)) || '-' == line.charAt(i) || '.' == line.charAt(i))) {
				return false;
			}
		}

		return true;
	}

	private static void printResults(BufferedWriter writer) throws Exception {
		String line = null;
		for (Iterator<String> iterator = hosts.keySet().iterator(); iterator.hasNext();) {
			line = iterator.next();
			writer.write(line + "\n");
		}
		writer.write("\n");
		for (Iterator<String> iterator = hostUrls.keySet().iterator(); iterator.hasNext();) {
			line = iterator.next();
			writer.write(line + hostUrls.get(line) + "\n");
		}
		writer.write("\n");
		for (Iterator<String> iterator = paths.keySet().iterator(); iterator.hasNext();) {
			line = iterator.next();
			writer.write(line + "\n");
		}
	}

	public static void main(String[] args) {
		// System.out.println(isLeagleHost("log.daqi.com"));
		loadADRecords(null);

		long start = System.currentTimeMillis();
		System.out
				.println(isAD(
						"http://mfp.deliver.ifeng.com/mfp/mfpMultipleDelivery.do?ADUNITID=257,258,259,260&CHANNEL=index&jsonp=MTf88f7f899d528f&USERID=1413347080816_h68h1n7210&PID="
								.toLowerCase(), "mfp.deliver.ifeng.com"));
		System.out.println(isAD("http://www.bidcenter.com.cn/user/fbzbgg.aspx".toLowerCase(), "www.bidcenter.com.cn"));

		System.out.println(System.currentTimeMillis() - start);
	}

}
