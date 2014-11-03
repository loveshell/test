package org.test.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;

import org.test.bean.PatternString;

/**
 * @author hwei.mike
 * 
 *         文件操作类
 * */
public class IOUtils {

	public String ReadHtml(File path, String encoding) {
		BufferedReader br;
		InputStreamReader is;
		String s = "";
		try {
			is = new InputStreamReader(new FileInputStream(path), encoding);
			br = new BufferedReader(is);

			while (br.ready()) {
				s += br.readLine();

			}
			br.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return s;
	}

	public ArrayList<PatternString> ReadPatternString(File path, String encoding) {
		BufferedReader br;
		InputStreamReader is;
		ArrayList<PatternString> PatternStringCommon = new ArrayList<PatternString>();
		try {
			is = new InputStreamReader(new FileInputStream(path), encoding);
			br = new BufferedReader(is);

			while (br.ready()) {
				String[] s = br.readLine().split("\t");
				if (s.length == 1) {
					break;
				}
				PatternString ps = new PatternString();
				ps.changeTime = Integer.parseInt(s[0]);
				ps.pattern = s[1];
				ps.relativePositionPercentage = Double.parseDouble(s[2]);

				PatternStringCommon.add(ps);
				ps = null;
			}
			br.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return PatternStringCommon;
	}

	public String Read(File path, String encoding) {
		BufferedReader br;
		InputStreamReader is;
		String s = "";
		try {
			is = new InputStreamReader(new FileInputStream(path), encoding);
			br = new BufferedReader(is);

			while (br.ready()) {
				s += br.readLine().trim();
			}
			br.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return s;
	}

	/**
	 * 写String
	 * 
	 * */
	public void Write(String path, String text, String encode, boolean isContinueWrite) {

		OutputStreamWriter writer;
		try {
			writer = new OutputStreamWriter(new FileOutputStream(path, isContinueWrite), encode);
			writer.write(text + "\n");
			writer.close();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
