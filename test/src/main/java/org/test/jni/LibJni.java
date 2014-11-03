package org.test.jni;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;

public class LibJni {

	private static final boolean isWindows = System.getProperty("os.name").toLowerCase().indexOf("windows") > -1;

	public static final String fileName = isWindows ? "filter.dll" : "filter.so";
	static {
		// ��Ҫ��������ļ�
		try {
			InputStream input = LibJni.class.getResourceAsStream(fileName);
			input.available();
			FileOutputStream out = new FileOutputStream(fileName);
			byte[] data = new byte[1024];
			int len = 0;
			while ((len = input.read(data)) > 0) {
				out.write(data, 0, len);
			}
			input.close();
			out.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		System.load(new File(fileName).getAbsolutePath());
	}

	public native byte[] extract(String strHtml);

	public native byte[] purify(String strHtml);

}