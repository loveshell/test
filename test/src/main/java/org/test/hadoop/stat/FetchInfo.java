package org.test.hadoop.stat;

public class FetchInfo {
	public String name = "";
	public long start = 0;
	public long end = 0;

	public long pageTotal = 0;
	public long pageSuccess = 0;
	public long downBytes = 0;
	public long fetchTime = 0;
	public long mapTime = 0;
	public long reduceTime = 0;

	@Override
	public String toString() {
		float avgPageSize = 0;
		if (pageSuccess > 0)
			avgPageSize = downBytes / pageSuccess / 1024;
		float avgFetchSpeed = 0;
		if (fetchTime > 0)
			avgFetchSpeed = 1000 * downBytes / fetchTime / 1024;
		float avgMapSpeed = 0;
		if (mapTime > 0)
			avgMapSpeed = 1000 * downBytes / mapTime / 1024;

		String result = "######################################\n" + "fetch=" + name + " pageTotal=" + pageTotal
				+ " pageSuccess=" + pageSuccess + " downBytes=" + downBytes + " fetchTime=" + fetchTime + " mapTime="
				+ mapTime + " reduceTime=" + reduceTime + "\n";
		String result2 = "avgPageSize=" + avgPageSize + "k avgFetchSpeed=" + avgFetchSpeed + "kbyte/s avgMapSpeed="
				+ avgMapSpeed + "kbyte/s";
		return result + result2;
	}

}