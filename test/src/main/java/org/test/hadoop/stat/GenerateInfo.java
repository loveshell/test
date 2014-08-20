package org.test.hadoop.stat;

public class GenerateInfo {
	public long start = 0;
	public long end = 0;
	public String endTime = "";
	public long generate = 0;
	public String table = "";

	@Override
	public String toString() {
		return endTime + ": generate: table=" + table + ", generate=" + generate + " timeused=" + (end - start) / 1000
				+ "s.";
	}

}
