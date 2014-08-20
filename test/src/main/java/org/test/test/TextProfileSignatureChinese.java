package org.test.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.MD5Hash;
import org.apache.nutch.crawl.MD5Signature;
import org.apache.nutch.crawl.Signature;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.NutchConfiguration;

public class TextProfileSignatureChinese extends Signature {
	Signature fallback = new MD5Signature();

	public byte[] calculate(Content content, Parse parse) {
		String text = null;
		if (parse != null)
			text = parse.getText();
		if (text == null || text.length() == 0)
			return fallback.calculate(content, parse);

		String pageType = parse.getData().getParseMeta().get("pageType");
		if ("1".equals(pageType))
			return MD5Hash.digest(calculateText(content, text)).getDigest();
		else
			return fallback.calculate(content, parse);
	}

	private String calculateText(Content content, String text) {
		HashMap<String, Token> tokens = new HashMap<String, Token>();
		int MIN_TOKEN_LEN = 1;
		// getConf().getInt("db.signature.text_profile.min_token_len", 2);
		float QUANT_RATE = getConf().getFloat("db.signature.text_profile.quant_rate", 0.01f);
		// 中文分词 String[] keys = AnalyzerUtil.getInstance().analyze(text);
		String[] keys = null;
		int maxFreq = 0;
		if (keys != null) {
			for (int i = 0; i < keys.length; i++) {
				String s = keys[i];
				if (s.length() > MIN_TOKEN_LEN) {
					// add it
					Token tok = tokens.get(s);
					if (tok == null) {
						tok = new Token(0, s);
						tokens.put(s, tok);
					}
					tok.cnt++;
					if (tok.cnt > maxFreq)
						maxFreq = tok.cnt;
				}
			}
		}

		// calculate the QUANT value
		int QUANT = Math.round(maxFreq * QUANT_RATE);
		if (QUANT < 2) {
			if (maxFreq > 1)
				QUANT = 2;
			else
				QUANT = 1;
		}

		Iterator<Token> it = tokens.values().iterator();
		List<Token> profile = new ArrayList<Token>();
		while (it.hasNext()) {
			Token t = it.next();
			// // round down to the nearest QUANT
			// t.cnt = (t.cnt / QUANT) * QUANT;
			// // discard the frequencies below the QUANT
			// // 只有至少发生2次的词才被计算
			// if (t.cnt < QUANT) {
			// // continue;
			// }
			profile.add(t);
		}
		// 按发生频率排序
		Collections.sort(profile, new TokenComparator());

		StringBuffer newText = new StringBuffer();
		it = profile.iterator();
		while (it.hasNext()) {
			Token t = it.next();
			if (newText.length() > 0)
				newText.append("\n");
			newText.append(t.toString());
		}
		return newText.toString();
	}

	private static class Token {
		public int cnt;
		public String val;

		public Token(int cnt, String val) {
			this.cnt = cnt;
			this.val = val;
		}

		public String toString() {
			return val + " " + cnt;
		}
	}

	private static class TokenComparator implements Comparator<Token> {
		public int compare(Token t1, Token t2) {
			if (t2.cnt == t1.cnt) {
				return t1.val.compareTo(t2.val);
			} else
				return t2.cnt - t1.cnt;
		}
	}

	public static void main(String[] args) throws Exception {
		TextProfileSignatureChinese sig = new TextProfileSignatureChinese();
		sig.setConf(NutchConfiguration.create());
		HashMap<String, String> res = new HashMap<String, String>();
		File[] files = new File(args[0]).listFiles();
		for (int i = 0; i < files.length; i++) {
			FileInputStream fis = new FileInputStream(files[i]);
			BufferedReader br = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
			StringBuffer text = new StringBuffer();
			String line = null;
			while ((line = br.readLine()) != null) {
				if (text.length() > 0)
					text.append("\n");
				text.append(line);
			}
			br.close();
			String signature = sig.calculateText(null, text.toString());
			res.put(files[i].toString(), signature);
		}
		Iterator<String> it = res.keySet().iterator();
		while (it.hasNext()) {
			String name = it.next();
			String signature = res.get(name);
			System.out.println(name + "\n" + signature);
		}
	}

}
