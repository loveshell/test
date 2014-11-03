package org.test.bean;

/**
 * 正文模板参数
 * @author kwu.Ganymede
 *
 */
public class PatternIndex {
	// 开始位置
	public int start;

	// 结束位置
	public int end;

	// 总内容长度
	public int contentLength;

	// 平均内容长度
	public double avgLength;

	// 该内容出现的次数
	public int count;

	// 是否为正文
	public boolean isArctile;
	
	//标题相似度
	public int likeTile;
	
	//平均标题相似度
	public double avgLikeTile;

	@Override
	public String toString() {
		return "PatternIndex [start=" + start + ", end=" + end + ", contentLength=" + contentLength + ", avgLength=" + avgLength + ", count=" + count + ", isArctile=" + isArctile + "]";
	}
	
}
