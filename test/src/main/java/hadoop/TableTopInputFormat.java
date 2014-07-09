package hadoop;

import hbase.HbaseClient.HostFilter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.nutch.crawl.CrawlDatum;

public class TableTopInputFormat implements InputFormat<Text, CrawlDatum> {

	private String table;

	public static class CustomInputSplit extends org.apache.hadoop.mapreduce.InputSplit implements InputSplit {
		private String tableName = null;

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
		int topn = 10000;
		int hostn = 50;

		List<Filter> tmp = new ArrayList<Filter>();
		Filter filter1 = new PageFilter(Integer.valueOf(topn).longValue());
		tmp.add(filter1);
		Filter filter2 = new HostFilter(hostn);
		tmp.add(filter2);
		FilterList filters = new FilterList(tmp);

		return new TableReader(job, table, topn, hostn, filters);
	}
}
