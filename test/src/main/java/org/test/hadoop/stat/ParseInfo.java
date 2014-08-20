package org.test.hadoop.stat;

public class ParseInfo {
	public String name = "";
	public long start = 0;
	public long end = 0;

	public long pageTotal = 0;
	public long parseSuccess = 0;
	public long parsePageByte = 0;
	public long parseTime = 0;
	public long fingerTime = 0;
	public long todoTime = 0;

	@Override
	public String toString() {
		float avgPageSec = 0;
		if (parseTime > 0)
			avgPageSec = 1000 * pageTotal / parseTime;
		float avgFingerTime = 0;
		if (parseSuccess > 0) {
			avgFingerTime = 100 * fingerTime / parseSuccess;
			avgFingerTime = avgFingerTime / 100;
		}
		String result = "######################################\n" + "parse=" + name + " pageTotal=" + pageTotal
				+ " parseSuccess=" + parseSuccess + " parsePageByte=" + parsePageByte + " parseTime=" + parseTime
				+ " fingerTime=" + fingerTime + " todoTime=" + todoTime + "\n";
		String result2 = "avgPageSec=" + avgPageSec + "cnt/s avgFingerTime=" + avgFingerTime + "/ms";
		return result + result2;
	}

}