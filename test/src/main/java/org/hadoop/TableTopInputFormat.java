package org.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Generator;
import org.apache.nutch.metadata.Nutch;
import org.test.hbase.HostFilter;

public class TableTopInputFormat implements InputFormat<Text, CrawlDatum> {

	private String table;

	public static class CustomInputSplit extends org.apache.hadoop.mapreduce.InputSplit implements InputSplit {
		private String tableName = null;

		public CustomInputSplit() {
			super();
		}

		public CustomInputSplit(String table) {
			super();
			this.tableName = table;
		}

		public void write(DataOutput out) throws IOException {
			out.writeUTF(tableName);
		}

		public void readFields(DataInput in) throws IOException {
			tableName = in.readUTF();
		}

		public long getLength() throws IOException {
			return 1;
		}

		public String[] getLocations() throws IOException {
			return new String[] { tableName };
		}

	}

	public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
		table = job.get("generate.table");
		return new CustomInputSplit[] { new CustomInputSplit(table) };
	}

	public RecordReader<Text, CrawlDatum> getRecordReader(InputSplit split, JobConf job, Reporter reporter)
			throws IOException {
		if (table == null)
			table = job.get("generate.table");

		long topn = job.getLong(Generator.GENERATOR_TOP_N, 10000);
		int hostn = 50;
		int intervalThreshold = job.getInt(Generator.GENERATOR_MIN_INTERVAL, -1);
		long curTime = job.getLong(Nutch.GENERATE_TIME_KEY, System.currentTimeMillis());

		List<Filter> tmp = new ArrayList<Filter>();
		// 抓取时间限制 // check fetch schedule
		SingleColumnValueFilter columnFilter = new SingleColumnValueFilter(Bytes.toBytes("cf1"),
				Bytes.toBytes("Fetchtime"), CompareOp.LESS_OR_EQUAL, Bytes.toBytes(curTime));
		columnFilter.setFilterIfMissing(true);
		tmp.add(columnFilter);
		// generate时间限制
		columnFilter = new SingleColumnValueFilter(Bytes.toBytes("cf1"), Bytes.toBytes(Nutch.GENERATE_TIME_KEY),
				CompareOp.LESS_OR_EQUAL, Bytes.toBytes(curTime - job.getLong(Generator.GENERATOR_DELAY, 1L) * 3600L
						* 24L * 1000L));
		tmp.add(columnFilter);
		// 抓取间隔限制 // consider only entries with a
		if (intervalThreshold > 0) {
			// retry (or fetch) interval lower than threshold
			columnFilter = new SingleColumnValueFilter(Bytes.toBytes("cf1"), Bytes.toBytes("FetchInterval"),
					CompareOp.LESS_OR_EQUAL, Bytes.toBytes(intervalThreshold));
			tmp.add(columnFilter);
		}
		//
		columnFilter = new SingleColumnValueFilter(Bytes.toBytes("cf1"), Bytes.toBytes("Score"),
				CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(0f));
		columnFilter.setFilterIfMissing(true);
		tmp.add(columnFilter);
		// topn限制
		Filter filter = new HostFilter(hostn);
		tmp.add(filter);
		// 记录数限制
		filter = new PageFilter(topn);
		tmp.add(filter);

		FilterList filters = new FilterList(tmp);// 有序
		return new TableReader(job, table, topn, filters);
	}
}
