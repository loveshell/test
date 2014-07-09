package hadoop;

import java.io.IOException;
import java.util.Iterator;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.nutch.crawl.CrawlDatum;

public class TableReader implements RecordReader<Text, CrawlDatum> {
	private JobConf job;
	private int topn = 10000;
	private int hostn = 50;
	private long current = 0;
	private long total = 0;

	private HConnection connection;
	private HTableInterface table;
	private ResultScanner rs;

	public TableReader(JobConf job, String table, int topn, int hostn, FilterList filters) {
		this.job = job;
		this.topn = topn;
		this.hostn = hostn;
		total = topn;

		HBaseConfiguration.merge(this.job, HBaseConfiguration.create(this.job));
		try {
			connection = HConnectionManager.createConnection(this.job);
			this.table = connection.getTable(table);
		} catch (IOException e) {
			e.printStackTrace();
		}

		init(filters);
	}

	private void init(FilterList filters) {
		Scan scan = new Scan();
		scan.setCaching(topn);
		scan.setFilter(filters);

		try {
			rs = table.getScanner(scan);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public boolean next(Text key, CrawlDatum value) throws IOException {
		if (rs == null) {
			current = total;
			return false;
		}
		for (Result r : rs) {
			if (r == null || r.isEmpty()) {
				current = total;
				return false;
			}

			key.set(r.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("url")));
			createValue(value, r);

			if (++current > topn) {
				total = current;
				break;
			}
			return true;
		}

		return false;
	}

	public void createValue(CrawlDatum datum, Result r) {
		NavigableMap<byte[], byte[]> map = r.getFamilyMap(Bytes.toBytes("cf1"));
		org.apache.hadoop.io.MapWritable metaData = new org.apache.hadoop.io.MapWritable();
		for (Iterator iterator = map.keySet().iterator(); iterator.hasNext();) {
			byte[] key = (byte[]) iterator.next();
			byte[] value = map.get(key);
			String skey = Bytes.toString(key);

			if ("Score".equals(skey)) {
				if (value != null)
					datum.setScore(Bytes.toFloat(value));
			} else if ("Status".equals(skey)) {
				if (value != null)
					datum.setStatus(value[0]);
			} else if ("Fetchtime".equals(skey)) {
				if (value != null)
					datum.setFetchTime(Bytes.toLong(value));
			} else if ("Retries".equals(skey)) {
				if (value != null)
					datum.setRetriesSinceFetch(value[0]);
			} else if ("FetchInterval".equals(skey)) {
				if (value != null)
					datum.setFetchInterval(Bytes.toInt(value));
			} else if ("Modifiedtime".equals(skey)) {
				if (value != null)
					datum.setModifiedTime(Bytes.toLong(value));
			} else if ("Signature".equals(skey)) {
				if (value != null)
					datum.setSignature(value);
			} else
				metaData.put(new Text(key), new Text(value));
		}
	}

	public Text createKey() {
		return new Text();
	}

	public CrawlDatum createValue() {
		return new CrawlDatum();
	}

	public long getPos() throws IOException {
		return current;
	}

	public void close() throws IOException {
		rs.close();
		table.close();
		connection.close();
	}

	public float getProgress() throws IOException {
		return current / total;
	}
}