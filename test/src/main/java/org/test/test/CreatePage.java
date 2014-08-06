package org.test.test;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;

public class CreatePage {

	public static void main(String[] args) {
		CreatePage creator = new CreatePage();
		// creator.init();
		try {
			// creator.creatPage();
			creator.createUrl2();
		} catch (Exception e) {
			e.printStackTrace();
		}
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

	private void createUrl() throws Exception {
		long bTotal = 100000000;// 1000*1000;
		String file = "d:/data/urls10000";
		BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(file));
		// for(long i =0;i < bTotal;i++){
		// // byte[] url =
		// ("http://10.200.5.83/"+(i%1000)+"/"+i+".html\n").getBytes();
		// byte[] url =
		// ("http://10.200.7.33/"+(i%1000)+"/"+i+".html\n").getBytes();
		// out.write(url, 0, url.length);
		// }
		// for(long i =0;i < bTotal;i++){
		// byte[] url =
		// ("http://aaa"+(i%300)+"/"+(i%1000)+"/"+i+".html\n").getBytes();//10.200.5.17
		// out.write(url, 0, url.length);
		// }
		// for(long i =0;i < bTotal;i++){
		// byte[] url =
		// ("http://bbb"+(i%300)+"/"+(i%1000)+"/"+i+".html\n").getBytes();//10.200.4.246
		// out.write(url, 0, url.length);
		// }
		// for(long i =0;i < bTotal;i++){
		// byte[] url =
		// ("http://ccc"+(i%300)+"/"+(i%1000)+"/"+i+".html\n").getBytes();//10.200.3.5
		// out.write(url, 0, url.length);
		// }
		for (long i = 0; i < bTotal; i++) {
			byte[] url = ("http://ddd" + (i % 300) + "/" + (i % 1000) + "/" + i + ".html\n").getBytes();// 10.200.6.140
			out.write(url, 0, url.length);
		}
		for (long i = 0; i < bTotal; i++) {
			byte[] url = ("http://eee" + (i % 300) + "/" + (i % 1000) + "/" + i + ".html\n").getBytes();// 10.200.6.134
			out.write(url, 0, url.length);
		}
		// for(long i =0;i < bTotal;i++){
		// byte[] url =
		// ("http://fff"+(i%300)+"/"+(i%1000)+"/"+i+".html\n").getBytes();//10.200.6.1
		// out.write(url, 0, url.length);
		// }
		// for(long i =0;i < bTotal;i++){
		// byte[] url =
		// ("http://ggg"+(i%300)+"/"+(i%1000)+"/"+i+".html\n").getBytes();//10.200.5.96
		// out.write(url, 0, url.length);
		// }
		// for(long i =0;i < bTotal;i++){
		// byte[] url =
		// ("http://hhh"+(i%300)+"/"+(i%1000)+"/"+i+".html\n").getBytes();//10.200.6.23
		// out.write(url, 0, url.length);
		// }
		// for(long i =0;i < bTotal;i++){
		// byte[] url =
		// ("http://iii"+(i%300)+"/"+(i%1000)+"/"+i+".html\n").getBytes();//10.200.6.101
		// out.write(url, 0, url.length);
		// }
		// for(long i =0;i < bTotal;i++){
		// byte[] url =
		// ("http://jjj"+(i%300)+"/"+(i%1000)+"/"+i+".html\n").getBytes();//10.200.2.144
		// out.write(url, 0, url.length);
		// }
		out.flush();
		out.close();

	}

	private void createUrl2() throws Exception {
		long bTotal = 100000;// 1000*1000;
		String file = "d:/urls12";
		BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(file));
		// for(long i =0;i < bTotal;i++){
		// byte[] url =
		// ("http://192.168.11.47/"+(i%1000)+"/"+i+".html\n").getBytes();
		// out.write(url, 0, url.length);
		// }
		// for(long i =0;i < bTotal;i++){
		// byte[] url =
		// ("http://192.168.11.46/"+(i%1000)+"/"+i+".html\n").getBytes();
		// out.write(url, 0, url.length);
		// }

		// for(long i =0;i < bTotal;i++){
		// // byte[] url =
		// ("http://10.200.5.83/"+(i%1000)+"/"+i+".html\n").getBytes();
		// byte[] url =
		// ("http://10.200.7.33/"+(i%1000)+"/"+i+".html\n").getBytes();
		// out.write(url, 0, url.length);
		// }
		// for(long i =0;i < bTotal;i++){
		// byte[] url =
		// ("http://10.200.3.5/"+(i%1000)+"/"+i+".html\n").getBytes();//
		// out.write(url, 0, url.length);
		// }

		//
		for (long i = 0; i < bTotal; i++) {
			byte[] url = ("http://10.200.7.225/" + (i % 1000) + "/" + i + ".html\n").getBytes();//
			out.write(url, 0, url.length);
		}
		for (long i = 0; i < bTotal; i++) {
			byte[] url = ("http://10.200.4.249/" + (i % 1000) + "/" + i + ".html\n").getBytes();//
			out.write(url, 0, url.length);
		}
		for (long i = 0; i < bTotal; i++) {
			byte[] url = ("http://10.200.4.136/" + (i % 1000) + "/" + i + ".html\n").getBytes();//
			out.write(url, 0, url.length);
		}
		for (long i = 0; i < bTotal; i++) {
			byte[] url = ("http://10.200.4.141/" + (i % 1000) + "/" + i + ".html\n").getBytes();//
			out.write(url, 0, url.length);
		}
		for (long i = 0; i < bTotal; i++) {
			byte[] url = ("http://10.200.4.196/" + (i % 1000) + "/" + i + ".html\n").getBytes();//
			out.write(url, 0, url.length);
		}
		// for(long i =0;i < bTotal;i++){
		// byte[] url =
		// ("http://10.200.7.22/"+(i%1000)+"/"+i+".html\n").getBytes();//
		// out.write(url, 0, url.length);
		// }
		for (long i = 0; i < bTotal; i++) {
			byte[] url = ("http://10.200.7.13/" + (i % 1000) + "/" + i + ".html\n").getBytes();//
			out.write(url, 0, url.length);
		}
		for (long i = 0; i < bTotal; i++) {
			byte[] url = ("http://10.200.4.232/" + (i % 1000) + "/" + i + ".html\n").getBytes();//
			out.write(url, 0, url.length);
		}
		for (long i = 0; i < bTotal; i++) {
			byte[] url = ("http://10.200.5.21/" + (i % 1000) + "/" + i + ".html\n").getBytes();//
			out.write(url, 0, url.length);
		}
		for (long i = 0; i < bTotal; i++) {
			byte[] url = ("http://10.200.5.227/" + (i % 1000) + "/" + i + ".html\n").getBytes();//
			out.write(url, 0, url.length);
		}
		for (long i = 0; i < bTotal; i++) {
			byte[] url = ("http://10.200.5.17/" + (i % 1000) + "/" + i + ".html\n").getBytes();//
			out.write(url, 0, url.length);
		}
		for (long i = 0; i < bTotal; i++) {
			byte[] url = ("http://10.200.6.207/" + (i % 1000) + "/" + i + ".html\n").getBytes();//
			out.write(url, 0, url.length);
		}
		for (long i = 0; i < bTotal; i++) {
			byte[] url = ("http://10.200.5.85/" + (i % 1000) + "/" + i + ".html\n").getBytes();//
			out.write(url, 0, url.length);
		}

		// for(long i =0;i < bTotal;i++){
		// byte[] url =
		// ("http://10.200.6.1/"+(i%1000)+"/"+i+".html\n").getBytes();//
		// out.write(url, 0, url.length);
		// }
		// for(long i =0;i < bTotal;i++){
		// byte[] url =
		// ("http://10.200.5.96/"+(i%1000)+"/"+i+".html\n").getBytes();//
		// out.write(url, 0, url.length);
		// }
		// for(long i =0;i < bTotal;i++){
		// byte[] url =
		// ("http://10.200.6.23/"+(i%1000)+"/"+i+".html\n").getBytes();//
		// out.write(url, 0, url.length);
		// }
		// for(long i =0;i < bTotal;i++){
		// byte[] url =
		// ("http://10.200.6.101/"+(i%1000)+"/"+i+".html\n").getBytes();//
		// out.write(url, 0, url.length);
		// }
		// for(long i =0;i < bTotal;i++){
		// byte[] url =
		// ("http://10.200.2.144/"+(i%1000)+"/"+i+".html\n").getBytes();//
		// out.write(url, 0, url.length);
		// }
		// for(long i =0;i < bTotal;i++){
		// byte[] url =
		// ("http://10.200.5.17/"+(i%1000)+"/"+i+".html\n").getBytes();//
		// out.write(url, 0, url.length);
		// }
		// for(long i =0;i < bTotal;i++){
		// byte[] url =
		// ("http://10.200.4.246/"+(i%1000)+"/"+i+".html\n").getBytes();//
		// out.write(url, 0, url.length);
		// }
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
