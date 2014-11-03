package org.test.bean;

/**
 * 公共子串
 * 
 * @author kwu.Ganymede
 *
 */
public class PatternString {
	// 公共子串内容
	public String pattern;

	// 公共子串在文章中的相对位置
	public double relativePositionPercentage;

	// 该公共子串出现的位置
	public int changeTime;

	@Override
	public String toString() {
		return "PatternString [pattern=" + pattern + ", relativePositionPercentage=" + relativePositionPercentage + ", changeTime=" + changeTime + "]";
	}

}
