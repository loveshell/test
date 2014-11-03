package org.test.test;

import org.junit.Test;
import org.test.model.GetContent;

public class MainTest {
	private static GetContent getContent = new GetContent();

	@Test
	public void testGetContent() {
//		getContent.patternTest("D:/MyWork/workspace/NewsContentRec/page/20141021192815", 10, 15);
//		getContent.patternTest("D:/MyWork/workspace/NewsContentRec/page/20141021192734", 10, 15);
//		getContent.patternTest("D:/MyWork/workspace/NewsContentRec/page/20141021192808", 10, 15);
		
//		getContent.patternTest("D:/MyWork/workspace/NewsContentRec/page/20141022141758", 10, 25);
		
//		getContent.patternTest("D:/MyWork/workspace/NewsContentRec/page/20141022141753", 10, 15);
		
		
		getContent.patternTest("D:/MyWork/workspace/NewsContentRec/page/1", 10, 15);
	}
	
	public static void main(String[] args) {
		getContent.patternTest("D:/MyWork/workspace/NewsContentRec/page/20141022132535", 10, 15);
	}

	@Test
	public void testString() {
		String str = "abc123";

		for (int i = str.length() - 1; i > 0; i--) {
			String temp = str.substring(0, i);
			System.out.println(temp);
		}
	}

	@Test
	public void testClean() {
		String htmlStr = "http://www.chbcnet.com/la/pic/site2/20140924/ade759d2ad746039fc44f57a109b4c67.jpg/enpproperty--> 　随着阿里巴巴在纽交所上市，连日来马云以及阿里巴巴创造的财富故事被台媒广泛关注。";

		if (htmlStr.indexOf("-->") != -1) {
			System.out.println(htmlStr.substring(htmlStr.indexOf(" ")));
		}
	}
	
	@Test
	public void testString1() {
		String str = "abc123";

		for (int i = 0; i < str.length(); i++) {
			String temp = str.substring( i);
			System.out.println(temp);
		}
	}
	
	@Test
	public void testString2() {
		String str = "abc123";

		str = str.substring(str.length()-1);
		
		System.out.println(str);
	}
	
	
	@Test
	public void testString3() {
		String str = "abc123";

		str = str.substring(0,str.length()-1);
		
		System.out.println(str);
	}
	
	
	@Test
	public void testString4(){
		String text = "asdfawer你好fwfecsadfqwerfwadcasdfwe你好rfsadfwefwqef你好weasdfwae";
		String end = "你好";
		int index2 = text.indexOf(end);
		
		for(int j = 0 ; j < 15; j++){
			System.out.println(index2);
			int result = text.indexOf(end, index2+end.length());
			System.out.println("result : " + result);
			if(result>0){
				index2 = result;
			}
		}
	}
	
	@Test
	public void testString5(){
		String text = "asdfawer你好fwfecsadfqwerfwadcasdfwe你好rfsadfwefwqef你好weasdfwae";
		String end = "你好";
		int index2 = text.indexOf(end);
		
		System.out.println("index2 " + index2);
		
		index2 = getEndIndex(text, end, index2);
		
		System.out.println("index2 " + index2);
	}
	
	public int getEndIndex(String text, String end, int index2) {
		int result = text.indexOf(end, index2+end.length());
		if (result == -1) {
			return index2;
		}
		return getEndIndex(text, end, result);
	}
}
