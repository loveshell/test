/**
 * 
 */
package org.test.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * content crawl_fetch crawl_parse parse_data
 * 
 * .logs crawldb1 crawldbIdx fetchFail hostdbInfo
 * 
 * @author xxhuang
 * 
 */
public class HDFSUtil {
	private static SimpleDateFormat dateSdf = new SimpleDateFormat("yyyyMMdd");
	private static Configuration conf = new Configuration();
	// /nutchdata/segments/20141017164659/content/201410171654/m_000000_0_p/index
	private static String seg = "/nutchdata/segments";

	private static void countFilesByDate(String cur) {
		if (StringUtils.isEmpty(cur))
			cur = dateSdf.format(new Date());
		long dayMs = 24 * 3600 * 1000l;
		try {
			Date day = dateSdf.parse(cur);
			long start = day.getTime();
			long end = start + dayMs;
			String pre = dateSdf.format(new Date(start - dayMs));
			FileSystem fs = FileSystem.get(conf);

			countFetch(fs, pre, cur, start, end);
			countParse(fs, pre, cur, start, end);
			countHbase(fs, "/hbase/.logs");
			countHbase(fs, "/hbase/fetchFail");
			countHbase(fs, "/hbase/hostdbInfo");
			countHbase(fs, "/hbase/crawldbIdx");
			long cnt = 0;
			cnt += countHbase(fs, "/hbase/crawldb1");
			cnt += countHbase(fs, "/hbase/crawldb2");
			cnt += countHbase(fs, "/hbase/crawldb3");

			System.out.println("crawldb=" + cnt / 1024 / 1024 / 1024);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void countFetch(FileSystem fs, final String pre, final String cur, long start, long end) {
		long cnt = 0l;
		try {
			FileStatus[] fss = fs.globStatus(new Path(seg + "/**/**/**/**/*"), new PathFilter() {
				public boolean accept(Path path) {
					String tmp = path.toString();
					if (tmp.indexOf("segments/" + pre) > -1 || tmp.indexOf("segments/" + cur) > -1)
						if (tmp.indexOf("/content/") > -1 || tmp.indexOf("/crawl_fetch/") > -1)
							return true;
					return false;
				}
			});
			for (int i = 0; i < fss.length; i++) {
				FileStatus file = fss[i];
				cnt += countFileSize(file, start, end);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("fetch=" + cnt / 1024 / 1024 / 1024);
	}

	private static void countParse(FileSystem fs, final String pre, final String cur, long start, long end) {
		long cnt = 0l;
		try {
			FileStatus[] fss = fs.globStatus(new Path(seg + "/**/**/**/**/*"), new PathFilter() {
				public boolean accept(Path path) {
					String tmp = path.toString();
					if (tmp.indexOf("segments/" + pre) > -1 || tmp.indexOf("segments/" + cur) > -1)
						if (tmp.indexOf("/crawl_parse/") > -1 || tmp.indexOf("/parse_data/") > -1)
							return true;
					return false;
				}
			});
			for (int i = 0; i < fss.length; i++) {
				FileStatus file = fss[i];
				cnt += countFileSize(file, start, end);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("parse=" + cnt / 1024 / 1024 / 1024);
	}

	private static long countHbase(FileSystem fs, String path) {
		long cnt = 0l;
		try {
			FileStatus[] fss = fs.globStatus(new Path(path + "/*"));
			for (int i = 0; i < fss.length; i++) {
				FileStatus file = fss[i];
				System.out.println(file.getPath());
				cnt += file.getLen();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println(path + "=" + cnt / 1024 / 1024 / 1024);
		return cnt;
	}

	private static long countFileSize(FileStatus file, long start, long end) {
		System.out.println(file.getPath());
		long rs = 0l;
		// if (!file.isDir()) {}

		long time = file.getModificationTime();
		if (time >= start && time < end)
			return file.getLen();

		return rs;
	}

	public static void main(String[] args) {
		if (args.length < 1) {
			help();
			return;
		}
		for (int i = 0; i < args.length; i++) {
			if ("--urlBlackClean".equals(args[i])) {
				if (i == args.length - 1)
					countFilesByDate(null);
				else
					countFilesByDate(args[++i]);
			} else {
				help();
			}
		}
	}

	private static void help() {
		System.out.println("useage: --method var");
	}
}
