package org.test.hadoop;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.metadata.Nutch;

public class TableReader implements RecordReader<Text, CrawlDatum> {
	private JobConf job;
	private long topn = 10000;
	private long current = 0;
	private long total = 0;
	private static long generateTime;

	private HConnection connection;
	private HTableInterface table;
	private ResultScanner rs;

	public TableReader(JobConf job, String tableName, long topn, FilterList filters) {
		this.job = job;
		this.topn = topn;
		total = topn;
		generateTime = job.getLong(Nutch.GENERATE_TIME_KEY, System.currentTimeMillis());

		HBaseConfiguration.merge(this.job, HBaseConfiguration.create(this.job));
		try {
			connection = HConnectionManager.createConnection(this.job);
			this.table = connection.getTable(tableName);
		} catch (IOException e) {
			e.printStackTrace();
		}

		init(filters);
	}

	private void init(FilterList filters) {
		Scan scan = new Scan();
		scan.setFilter(filters);
		if (topn > 10000)
			scan.setCaching(10000);
		else
			scan.setCaching(Long.valueOf(topn).intValue());

		try {
			rs = table.getScanner(scan);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void createValue(CrawlDatum datum, Result r) {
		NavigableMap<byte[], byte[]> map = r.getFamilyMap(Bytes.toBytes("cf1"));
		org.apache.hadoop.io.MapWritable metaData = new org.apache.hadoop.io.MapWritable();
		datum.setMetaData(metaData);

		for (Iterator iterator = map.keySet().iterator(); iterator.hasNext();) {
			byte[] key = (byte[]) iterator.next();
			byte[] value = map.get(key);
			String skey = Bytes.toString(key);

			if ("url".equals(skey)) {
				// nothing
			} else if ("Score".equals(skey)) {
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
		metaData.put(new Text("urlid"), new Text(r.getRow()));
	}

	public static Put generatedPut(byte[] url, CrawlDatum value) {
		Put put = createPut(url, value);
		byte m = 1;
		// generate time
		put.add(Bytes.toBytes("cf1"), Bytes.toBytes(Nutch.GENERATE_TIME_KEY), Bytes.toBytes(generateTime));

		return put;
	}

	public static Put createPut(byte[] url, CrawlDatum value) {
		MapWritable meta = value.getMetaData();
		Text key = (Text) meta.get(new Text("urlid"));
		Put put = new Put(key.getBytes());

		put.add(Bytes.toBytes("cf1"), Bytes.toBytes("url"), url);
		put.add(Bytes.toBytes("cf1"), Bytes.toBytes("Score"), Bytes.toBytes(value.getScore()));
		put.add(Bytes.toBytes("cf1"), Bytes.toBytes("Status"), new byte[] { value.getStatus() });
		put.add(Bytes.toBytes("cf1"), Bytes.toBytes("Fetchtime"), Bytes.toBytes(value.getFetchTime()));
		put.add(Bytes.toBytes("cf1"), Bytes.toBytes("Retries"), new byte[] { value.getRetriesSinceFetch() });
		put.add(Bytes.toBytes("cf1"), Bytes.toBytes("FetchInterval"), Bytes.toBytes(value.getFetchInterval()));
		put.add(Bytes.toBytes("cf1"), Bytes.toBytes("Modifiedtime"), Bytes.toBytes(value.getModifiedTime()));
		if (value.getSignature() != null && value.getSignature().length != 0)
			put.add(Bytes.toBytes("cf1"), Bytes.toBytes("Signature"), value.getSignature());

		for (Entry<Writable, Writable> e : meta.entrySet()) {
			if (!"urlid".equals(((Text) e.getKey()).toString()))
				put.add(Bytes.toBytes("cf1"), Bytes.toBytes(e.getKey().toString()),
						Bytes.toBytes(e.getValue().toString()));
		}

		return put;
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