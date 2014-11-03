package org.test.test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HTMLSpirit {
    
    private static final String regEx_script = "<script[^>]*?>[\\s\\S]*?<\\/script>"; // 定义script的正则表达式
    private static final String regEx_style = "<style[^>]*?>[\\s\\S]*?<\\/style>"; // 定义style的正则表达式
    private static final String regEx_html = "<[^>]+>"; // 定义HTML标签的正则表达式
    
    public static String delHTMLTag(String htmlStr) {
        Pattern p_script = Pattern.compile(regEx_script, Pattern.CASE_INSENSITIVE);
        Matcher m_script = p_script.matcher(htmlStr);
        htmlStr = m_script.replaceAll(""); // 过滤script标签

        Pattern p_style = Pattern.compile(regEx_style, Pattern.CASE_INSENSITIVE);
        Matcher m_style = p_style.matcher(htmlStr);
        htmlStr = m_style.replaceAll(""); // 过滤style标签

        Pattern p_html = Pattern.compile(regEx_html, Pattern.CASE_INSENSITIVE);
        Matcher m_html = p_html.matcher(htmlStr);
        htmlStr = m_html.replaceAll(""); // 过滤html标签

        return htmlStr.trim(); // 返回文本字符串
    }
    
    public static void main(String[] args) {
		String htmlStr = "http://www.chbcnet.com/la/pic/site2/20140924/ade759d2ad746039fc44f57a109b4c67.jpg/enpproperty--> <img> <test>wewwr</test>　　随着阿里巴巴在纽交所上市，连日来马云以及阿里巴巴创造的财富故事被台媒广泛关注。";
		String delHTMLTag = delHTMLTag(htmlStr);
		System.out.println(delHTMLTag);
	}
    
}