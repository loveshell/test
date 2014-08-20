package org.test.test;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;

public class CreatePage {

	public static void main(String[] args) throws Exception {
		CreatePage creator = new CreatePage();
		// creator.init();
		// creator.creatPage();
		creator.createUrl2();
	}

	private String txt = null;

	private void init() {
		StringBuffer sb = new StringBuffer("");
		try {
			BufferedReader in = new BufferedReader(new FileReader("d:/test.htm"));
			String line = null;
			while ((line = in.readLine()) != null) {
				sb.append(line);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		txt = sb.toString();
	}

	private void createUrl2() throws Exception {
		long bTotal = 100000;// 1000*1000;
		String file = "d:/urls";
		BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(file));

		String[] ips = { "10.200.5.227", "10.200.5.21", "10.200.4.232", "10.200.4.141", "10.200.4.196", "10.200.7.13",
				"10.200.4.249", "10.200.5.85", "10.200.7.22", "10.200.5.17", "10.200.7.225", "10.200.4.136",
				"10.200.6.207" };
		String[] hosts = { "xxhuang", "kfang-felton", "sjwu", "wtong", "kwu-ganymede", "qwang-forward", "hwei-mike",
				"mfang-nj", "zning", "qzhang-john", "zddu", "xjyan" };

		for (int j = 0; j < hosts.length; j++) {
			for (long i = 0; i < bTotal; i++) {
				byte[] url = ("http://" + hosts[j] + "/" + (i % 1000) + "/" + i + ".html\n").getBytes();//
				out.write(url, 0, url.length);
			}
		}
		out.flush();

		for (int j = 0; j < ips.length; j++) {
			for (long i = 0; i < bTotal; i++) {
				byte[] url = ("http://" + ips[j] + "/" + (i % 1000) + "/" + i + ".html\n").getBytes();//
				out.write(url, 0, url.length);
			}
		}
		out.flush();
		out.close();
	}

	private void creatPage() throws Exception {
		long bTotal = 100005;// 1000*1000;
		String path = "d:/data/";
		for (long i = 0; i < bTotal; i++) {
			String fileName = i + ".html";
			String foldName = (i % 1000) + "";
			File fold = new File(path + foldName);
			if (!fold.exists()) {
				fold.mkdir();
			}
			File htmlFile = new File(path + foldName + "/" + fileName);
			if (!htmlFile.exists()) {
				FileWriter fw = new FileWriter(htmlFile);
				fw.write(txt);
				fw.flush();
				fw.close();
			}
			System.out.println(fileName);
		}
	}
}
