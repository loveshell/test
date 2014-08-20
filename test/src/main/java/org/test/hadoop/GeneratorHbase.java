/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.test.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Generator;
import org.apache.nutch.crawl.URLPartitioner;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.URLFilterException;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.TimingUtil;
// rLogging imports
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.test.hadoop.stat.GenerateInfo;
import org.test.hadoop.stat.GenerateInfos;
import org.test.hbase.HostFilter;

/**
 * Generates a subset of a crawl db to fetch. This version allows to generate
 * fetchlists for several segments in one go. Unlike in the initial version
 * (OldGenerator), the IP resolution is done ONLY on the entries which have been
 * selected for fetching. The URLs are partitioned by IP, domain or host within
 * a segment. We can chose separately how to count the URLS i.e. by domain or
 * host to limit the entries.
 **/
/**
 * Generates a subset of a crawl db to fetch. This version allows to generate
 * fetchlists for several segments in one go. Unlike in the initial version
 * (OldGenerator), the IP resolution is done ONLY on the entries which have been
 * selected for fetching. The URLs are partitioned by IP, domain or host within
 * a segment. We can chose separately how to count the URLS i.e. by domain or
 * host to limit the entries.
 **/
public class GeneratorHbase extends Generator {
	public static final Logger LOG = LoggerFactory.getLogger(GeneratorHbase.class);
	static final String GENERATL_TABLE = "generate.table";
	static final String GENERATL_REDUCECNT = "generate.reduceCnt";
	static int tableCacheSize = 100000;
	static int HBASE_REGIONSERVER_LEASE_PERIOD = 600000;

	private static Put createGenerateTime(byte[] url, CrawlDatum value, long generateTime) {
		Put put = createPutByDatum(url, value);
		// generate time
		put.add(Bytes.toBytes("cf1"), Bytes.toBytes(Nutch.GENERATE_TIME_KEY), Bytes.toBytes(generateTime));

		return put;
	}

	private static void createDatum(CrawlDatum datum, Result r) {
		NavigableMap<byte[], byte[]> map = r.getFamilyMap(Bytes.toBytes("cf1"));
		org.apache.hadoop.io.MapWritable metaData = new org.apache.hadoop.io.MapWritable();

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
		datum.setMetaData(metaData);
	}

	private static Put createPutByDatum(byte[] url, CrawlDatum value) {
		MapWritable meta = value.getMetaData();
		Text key = null;
		for (Entry<Writable, Writable> e : meta.entrySet()) {
			if ("urlid".equals(((Text) e.getKey()).toString())) {
				key = (Text) e.getValue();
				break;
			}
		}
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

	public static class TableTopInputFormat implements InputFormat<Text, CrawlDatum> {
		public static class TableReader implements RecordReader<Text, CrawlDatum> {
			private JobConf job;
			private long current = 0;
			private long total = Long.MAX_VALUE;

			Reporter reporter;
			private HConnection connection;
			private HTableInterface table;
			private ResultScanner rs;

			public TableReader(JobConf job, String tableName, FilterList filters, String start, String end,
					Reporter reporter) {
				this.job = job;
				this.reporter = reporter;

				HBaseConfiguration.merge(this.job, HBaseConfiguration.create(this.job));
				job.setLong(HConstants.HBASE_REGIONSERVER_LEASE_PERIOD_KEY, HBASE_REGIONSERVER_LEASE_PERIOD);
				try {
					connection = HConnectionManager.createConnection(this.job);
					this.table = connection.getTable(tableName);
				} catch (IOException e) {
					e.printStackTrace();
				}

				reporter.setStatus("startkey=" + start + " endKey=" + end);
				init(filters, start, end);
			}

			private void init(FilterList filters, String start, String end) {
				Scan scan = new Scan();
				scan.setFilter(filters);
				scan.setCaching(tableCacheSize);
				if (start != null && !start.isEmpty())
					scan.setStartRow(Bytes.toBytes(start));
				if (end != null && !end.isEmpty())
					scan.setStopRow(Bytes.toBytes(end));

				try {
					rs = table.getScanner(scan);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			//
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
					if (++current <= total) {
						byte[] urlByte = r.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("url"));
						key.set(urlByte);
						createDatum(value, r);

						return true;
					}
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
				if (rs != null)
					rs.close();
				table.close();
				connection.close();
			}

			public float getProgress() throws IOException {
				return current / 50000000;
			}
		}

		protected static String[] getHostSplits() {
			int numSplits = 51;
			String wwwPre = "www.";
			char a = 'a';
			String[] splits = new String[numSplits];
			for (int i = 0; i < 22; i++) {
				splits[i] = String.valueOf(a++);
			}
			a = 'a';
			for (int i = 0; i < 26; i++) {
				StringBuilder sb = new StringBuilder(wwwPre).append(a++);
				splits[22 + i] = sb.toString();
			}
			splits[48] = "x";
			splits[49] = "y";
			splits[50] = "z";
			return splits;
		}

		public static class TableKeyInputSplit extends org.apache.hadoop.mapreduce.InputSplit implements InputSplit {
			private String tableName = null;
			private String begin = "";
			private String end = "";

			public String getBegin() {
				return begin;
			}

			public void setBegin(String begin) {
				this.begin = begin;
			}

			public String getEnd() {
				return end;
			}

			public void setEnd(String end) {
				this.end = end;
			}

			public TableKeyInputSplit() {
			}

			public TableKeyInputSplit(String table, String begin, String end) {
				this.tableName = table;
				this.begin = begin;
				this.end = end;
			}

			public void write(DataOutput out) throws IOException {
				out.writeUTF(tableName);
				out.writeUTF(begin);
				out.writeUTF(end);
			}

			public void readFields(DataInput in) throws IOException {
				tableName = in.readUTF();
				begin = in.readUTF();
				end = in.readUTF();
			}

			public long getLength() throws IOException {
				return 1;
			}

			public String[] getLocations() throws IOException {
				return new String[] { tableName };
			}
		}

		private String table;

		public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
			table = job.get(GENERATL_TABLE);
			String[] hostSplits = getHostSplits();
			List<TableKeyInputSplit> list = new ArrayList<TableKeyInputSplit>();
			for (int i = 0; i < hostSplits.length; i++) {
				TableKeyInputSplit split = null;
				if (i == 0)
					split = new TableKeyInputSplit(table, "!", hostSplits[0]);
				else if (i == hostSplits.length - 1)
					split = new TableKeyInputSplit(table, hostSplits[i], "~");
				else
					split = new TableKeyInputSplit(table, hostSplits[i - 1], hostSplits[i]);

				list.add(split);
			}

			return list.toArray(new TableKeyInputSplit[] {});
		}

		public InputSplit[] getSplitsBak(JobConf job, int numSplits) throws IOException {
			table = job.get(GENERATL_TABLE);
			int reduceCnt = job.getInt(GENERATL_REDUCECNT, 8);
			String[] hostSplits = getHostSplits();
			int len = Math.round(Integer.valueOf(hostSplits.length).floatValue()
					/ Integer.valueOf(reduceCnt).floatValue());
			List<TableKeyInputSplit> list = new ArrayList<TableKeyInputSplit>();
			for (int i = 0; i < reduceCnt; i++) {
				TableKeyInputSplit split = null;
				if (i == 0)
					split = new TableKeyInputSplit(table, "", hostSplits[len - 1]);
				else if (i == reduceCnt - 1)
					split = new TableKeyInputSplit(table, hostSplits[len * i - 1], "");
				else
					split = new TableKeyInputSplit(table, hostSplits[len * i - 1], hostSplits[len * (i + 1) - 1]);

				list.add(split);
			}

			return list.toArray(new TableKeyInputSplit[] {});
		}

		public RecordReader<Text, CrawlDatum> getRecordReader(InputSplit split, JobConf job, Reporter reporter)
				throws IOException {
			if (table == null)
				table = job.get("generate.table");

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
					CompareOp.LESS_OR_EQUAL, Bytes.toBytes(curTime
							- job.getLong(Generator.GENERATOR_DELAY, 24 * 3600 * 1000l)));
			tmp.add(columnFilter);
			// 抓取间隔限制 // consider only entries with a
			if (intervalThreshold > 0) {
				// retry (or fetch) interval lower than threshold
				columnFilter = new SingleColumnValueFilter(Bytes.toBytes("cf1"), Bytes.toBytes("FetchInterval"),
						CompareOp.LESS_OR_EQUAL, Bytes.toBytes(intervalThreshold));
				tmp.add(columnFilter);
			}

			FilterList filters = new FilterList(tmp);
			return new TableReader(job, table, filters, ((TableKeyInputSplit) split).getBegin(),
					((TableKeyInputSplit) split).getEnd(), reporter);
		}
	}

	public static class CodeInputFormat implements InputFormat<IntWritable, IntWritable> {
		public static class CodeReader implements RecordReader<IntWritable, IntWritable> {
			private int current = 0;
			private int reduceCnt = 1;

			public CodeReader(int reduceCnt) {
				super();
				this.reduceCnt = reduceCnt;
			}

			public boolean next(IntWritable key, IntWritable value) throws IOException {
				if (current == reduceCnt)
					return false;

				key.set(current);
				value.set(current);
				current++;

				return true;
			}

			public IntWritable createKey() {
				return new IntWritable(0);
			}

			public IntWritable createValue() {
				return new IntWritable(0);
			}

			public long getPos() throws IOException {
				return current;
			}

			public void close() throws IOException {
			}

			public float getProgress() throws IOException {
				return current / reduceCnt;
			}
		}

		public static class CustomInputSplit extends org.apache.hadoop.mapreduce.InputSplit implements InputSplit {
			private int reduceCnt = 1;

			public CustomInputSplit() {
				super();
			}

			public CustomInputSplit(int reduceCnt) {
				super();
				this.reduceCnt = reduceCnt;
			}

			public void write(DataOutput out) throws IOException {
				out.writeInt(reduceCnt);
			}

			public void readFields(DataInput in) throws IOException {
				reduceCnt = in.readInt();
			}

			public long getLength() throws IOException {
				return reduceCnt;// reduce num?
			}

			public String[] getLocations() throws IOException {
				return new String[] {};
			}
		}

		public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
			return new CustomInputSplit[] { new CustomInputSplit(job.getInt(GeneratorHbase.GENERATL_REDUCECNT, 1)) };
		}

		public RecordReader<IntWritable, IntWritable> getRecordReader(InputSplit split, JobConf job, Reporter reporter)
				throws IOException {
			return new CodeReader(job.getInt(GeneratorHbase.GENERATL_REDUCECNT, 1));
		}
	}

	/** Selects entries due for fetch. */
	public static class GenerateMark implements Mapper<Text, CrawlDatum, Text, CrawlDatum>,
			Partitioner<Text, CrawlDatum>, Reducer<IntWritable, IntWritable, Text, CrawlDatum> {
		private boolean filter;
		private URLFilters filters;
		JobConf conf;
		private Map hostCnt = new HashMap();
		private long topn = tableCacheSize;
		private int hostn = -1;
		private long generateTime;
		private int reduceCnt = 1;
		private int seed;
		private Partitioner<Text, Writable> partitioner = new URLPartitioner();

		private HConnection connection;
		private HTableInterface table;
		private long cnt = 0;

		private long curTimeMillis = 0;
		private long lastGenTimeMillis = 0;
		private boolean isSmart = false;

		static long GetZeroTimeSeconds() {
			Calendar c = Calendar.getInstance();
			try {
				c.setTime(new SimpleDateFormat("yyyy-MM-dd").parse("2014-01-01"));
			} catch (ParseException e) {
				e.printStackTrace();
			}
			return c.getTimeInMillis() / 1000L;
		}

		static final long ZeroTimeMillis = GetZeroTimeSeconds() * 1000L; // 单位秒;

		public void configure(JobConf job) {
			conf = job;
			filter = job.getBoolean(GENERATOR_FILTER, true);
			if (filter)
				filters = new URLFilters(job);

			reduceCnt = job.getInt(GeneratorHbase.GENERATL_REDUCECNT, 1);
			topn = job.getLong(Generator.GENERATOR_TOP_N, tableCacheSize);
			hostn = job.getInt(Generator.GENERATOR_MAX_COUNT, -1);
			generateTime = job.getLong(Nutch.GENERATE_TIME_KEY, System.currentTimeMillis());
			seed = job.getInt("partition.url.seed", 0);
			partitioner.configure(job);

			String tableName = job.get(GENERATL_TABLE);
			HBaseConfiguration.merge(conf, HBaseConfiguration.create(conf));
			try {
				connection = HConnectionManager.createConnection(job);
				table = connection.getTable(tableName);
				table.setAutoFlush(false, true);
				table.setWriteBufferSize(300 * tableCacheSize);
			} catch (IOException e) {
				e.printStackTrace();
			}

			isSmart = job.getBoolean("nutch.smart.is", false);
			if (isSmart) {
			}
		}

		public void close() {
			try {
				table.flushCommits();
				table.close();
				connection.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

			LOG.info("total=" + cnt + "; " + hostCnt);
		}

		public void map(Text key, CrawlDatum value, OutputCollector<Text, CrawlDatum> output, Reporter reporter)
				throws IOException {
			if (filter) {
				try {
					if (filters.filter(key.toString()) == null)
						return;
				} catch (URLFilterException e) {
					if (LOG.isWarnEnabled()) {
						LOG.warn("Couldn't filter url: " + key + " (" + e.getMessage() + ")");
					}
				}
			}

			if (!hostFilte(key.toString()))
				return;

			Put put = createGenerateTime(key.getBytes(), value, generateTime);
			table.put(put);
			if (++cnt % tableCacheSize == 0) {
				table.flushCommits();
				reporter.setStatus("commit:" + cnt);
			}

			output.collect(key, value);// 收集一个，partition一个

			reporter.incrCounter("Generator", "records", 1);
		}

		private boolean hostFilte(String url) {
			String host = getHost(url);
			if (host != null && !host.isEmpty()) {
				if (hostCnt.containsKey(host)) {
					AtomicLong cnt = (AtomicLong) hostCnt.get(host);
					cnt.incrementAndGet();
				} else {
					hostCnt.put(host, new AtomicLong(1));
				}
				return true;
			}
			return false;
		}

		@Override
		public void reduce(IntWritable key, Iterator<IntWritable> values, OutputCollector<Text, CrawlDatum> output,
				Reporter reporter) throws IOException {
			LOG.info("generatorHbase:load url from partition=" + key.get());

			int part = key.get();
			long partTopn = topn / reduceCnt;// part topn

			ResultScanner rs = getRS();
			if (rs == null) {
				return;
			}

			for (Result r : rs) {
				if (cnt == partTopn) {
					return;
				}
				if (r == null || r.isEmpty()) {
					return;
				}

				byte[] urlByte = r.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("url"));
				if (!filteUrl(Bytes.toString(urlByte), part)) {
					continue;
				}

				Text urlKey = new Text(urlByte);
				CrawlDatum value = new CrawlDatum();
				createDatum(value, r);

				// if (!smartFilter(value))
				// continue;

				Put put = createGenerateTime(urlKey.getBytes(), value, generateTime);
				table.put(put);
				if (++cnt % 10000 == 0) {
					table.flushCommits();
				}

				output.collect(urlKey, value);// OldOutputCollector
				reporter.incrCounter("Generator", "records", 1);
			}

		}

		private boolean smartFilter(CrawlDatum value) throws IOException {
			if (isSmart) {// 是否是特殊网站抓取
				// 获取在[lastGenTime,curTime]之间应该抓取的URL
				int interval = value.getFetchInterval();
				if (curTimeMillis / interval <= lastGenTimeMillis / interval)
					return false;// 不满足抓取条件
			}

			return true;
		}

		private boolean filteUrl(String url, int part) {
			if (filter) {
				// don't generate URLs that don't pass URLFilters
				try {
					if (filters.filter(url) == null)
						return false;
				} catch (URLFilterException e) {
					if (LOG.isWarnEnabled()) {
						LOG.warn("Couldn't filter url: " + url + " (" + e.getMessage() + ")");
					}
					return false;
				}
			}

			String host = getHost(url);
			if (part != getHashPartition(url, host))// belong to the partition?
				return false;

			if (host != null) {
				if (hostCnt.containsKey(host)) {
					AtomicLong cnt = (AtomicLong) hostCnt.get(host);
					cnt.incrementAndGet();

					if (hostn != -1) {
						if (cnt.get() <= hostn)
							return true;
						else {
							cnt.decrementAndGet();
							return false;
						}
					}
					return true;
				} else {
					hostCnt.put(host, new AtomicLong(1));
					return true;
				}
			}

			return false;
		}

		private String getHost(String url) {
			String host = null;
			try {
				URL tmp = new URL(url);
				host = tmp.getHost();
			} catch (MalformedURLException e) {
				e.printStackTrace();
			}
			return host;
		}

		/** Hash by domain name. */
		private int getHashPartition(String urlString, String host) {
			int hashCode = urlString.hashCode();
			if (host != null)
				hashCode = host.hashCode();
			// make hosts wind up in different partitions on different runs
			hashCode ^= seed;

			int part = (hashCode & Integer.MAX_VALUE) % reduceCnt;
			return part;
		}

		private ResultScanner getRS() throws IOException {
			ResultScanner rs = null;

			Scan scan = new Scan();
			scan.setFilter(getFilters());
			// if (topn > rsCache)
			// scan.setCaching(Long.valueOf(topn / 2).intValue());
			// else
			scan.setCaching(Long.valueOf(topn).intValue());

			try {
				rs = table.getScanner(scan);
			} catch (IOException e) {
				e.printStackTrace();
			}

			return rs;
		}

		private FilterList getFilters() throws IOException {
			int intervalThreshold = conf.getInt(Generator.GENERATOR_MIN_INTERVAL, -1);

			List<Filter> tmp = new ArrayList<Filter>();
			// 抓取时间限制 // check fetch schedule
			SingleColumnValueFilter columnFilter = new SingleColumnValueFilter(Bytes.toBytes("cf1"),
					Bytes.toBytes("Fetchtime"), CompareOp.LESS_OR_EQUAL, Bytes.toBytes(generateTime));
			columnFilter.setFilterIfMissing(true);
			tmp.add(columnFilter);
			// generate时间限制
			columnFilter = new SingleColumnValueFilter(Bytes.toBytes("cf1"), Bytes.toBytes(Nutch.GENERATE_TIME_KEY),
					CompareOp.LESS_OR_EQUAL, Bytes.toBytes(generateTime
							- conf.getLong(Generator.GENERATOR_DELAY, 24 * 3600 * 1000l)));
			tmp.add(columnFilter);
			// 抓取间隔限制 // consider only entries with a
			if (intervalThreshold > 0) {
				// retry (or fetch) interval lower than threshold
				columnFilter = new SingleColumnValueFilter(Bytes.toBytes("cf1"), Bytes.toBytes("FetchInterval"),
						CompareOp.LESS_OR_EQUAL, Bytes.toBytes(intervalThreshold));
				tmp.add(columnFilter);
			}

			// columnFilter = new SingleColumnValueFilter(Bytes.toBytes("cf1"),
			// Bytes.toBytes("Score"),
			// CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(0f));
			// columnFilter.setFilterIfMissing(true);
			// tmp.add(columnFilter);

			if (hostn > 0) {
				// topn限制
				Filter filter = new HostFilter(hostn);
				tmp.add(filter);
			}
			// 记录数限制
			Filter filter = new PageFilter(topn);
			tmp.add(filter);

			FilterList filters = new FilterList(tmp);
			return filters;
		}

		@Override
		public int getPartition(Text key, CrawlDatum value, int numPartitions) {
			return partitioner.getPartition(key, value, numPartitions);
		}
	}

	public GeneratorHbase() {
	}

	public GeneratorHbase(Configuration conf) {
		setConf(conf);
	}

	public Path generateAll(int tableNum, Path segments, int reduceCnt, boolean filter, boolean norm, boolean force)
			throws IOException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		long start = System.currentTimeMillis();
		LOG.info("Generator: starting at " + sdf.format(start));
		LOG.info("Generator: Selecting best-scoring urls due for fetch.");
		LOG.info("Generator: filtering: " + filter);
		LOG.info("Generator: normalizing: " + norm);
		if ("true".equals(getConf().get(GENERATE_MAX_PER_HOST_BY_IP))) {
			LOG.info("Generator: GENERATE_MAX_PER_HOST_BY_IP will be ignored, use partition.url.mode instead");
		}

		Path segment = new Path(segments, Generator.generateSegmentName());
		long begin = System.currentTimeMillis();
		String table = "crawldb" + tableNum;
		RunningJob r = generateJob(table, segment, reduceCnt, filter, norm, force);
		Counter counter = r.getCounters().findCounter("Generator", "records");
		long cnt = counter.getValue();
		LOG.info("Generator: " + segment + " records: " + cnt + " current table=" + table + " timeused="
				+ (System.currentTimeMillis() - begin) / 1000 + "s");
		if (cnt == 0) {
			removePath(FileSystem.get(getConf()), segment);
			return null;
		}

		long end = System.currentTimeMillis();
		// have records
		GenerateInfo genInfo = GenerateInfos.getGenerateInfo();
		genInfo.start = begin;
		genInfo.generate = cnt;
		genInfo.table = table;
		genInfo.end = end;
		genInfo.endTime = sdf.format(genInfo.end);
		LOG.info(GenerateInfos.printString());
		LOG.info("Generator: finished at " + sdf.format(end) + ", elapsed: " + TimingUtil.elapsedTime(start, end));

		return segment;
	}

	public static boolean removePath(FileSystem fs, Path segment) {
		try {
			if (!fs.exists(segment))
				return false;
			return fs.delete(segment, true);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	private RunningJob generateJob(String table, Path segment, int reduceCnt, boolean filter, boolean norm,
			boolean force) throws IOException {
		LOG.info("Generator: segment=" + segment);

		JobConf job = new NutchJob(getConf());
		job.setJarByClass(GeneratorHbase.class);
		job.setJobName("generate: from " + table + " "
				+ (new SimpleDateFormat("MMdd HH:mm:ss")).format(System.currentTimeMillis()));

		if (reduceCnt == -1) {
			reduceCnt = job.getNumMapTasks(); // a partition per fetch task
		}
		if ("local".equals(job.get("mapred.job.tracker")) && reduceCnt != 1) {
			LOG.info("Generator: jobtracker is 'local', generating exactly one partition.");
			reduceCnt = 1;
		}
		// job.setLong(GENERATOR_CUR_TIME, curTime);
		// record real generation time
		long generateTime = System.currentTimeMillis();
		job.setLong(Nutch.GENERATE_TIME_KEY, generateTime);
		job.setBoolean(GENERATOR_FILTER, filter);
		job.setBoolean(GENERATOR_NORMALISE, norm);
		job.set(GENERATL_TABLE, table);
		job.setInt(GENERATL_REDUCECNT, reduceCnt);
		job.setInt("partition.url.seed", new Random().nextInt());

		job.setInputFormat(TableTopInputFormat.class);// 查询数据
		job.setMapperClass(GenerateMark.class);// 标记generate?

		job.setPartitionerClass(GenerateMark.class);
		job.setNumReduceTasks(reduceCnt);
		job.setOutputFormat(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(CrawlDatum.class);
		job.setOutputKeyComparatorClass(HashComparator.class);
		Path output = new Path(segment, CrawlDatum.GENERATE_DIR_NAME);
		FileOutputFormat.setOutputPath(job, output);

		RunningJob r = null;
		try {
			r = JobClient.runJob(job);
		} catch (IOException e) {
			throw e;
		}
		return r;
	}

	public Path[] generate(Path segments, int reduceCnt, long topN, boolean filter, boolean norm, boolean force,
			int segCount, int tableDepth) throws IOException {
		GenerateInfos.topn = topN;
		GenerateInfos.hostn = getConf().getInt(Generator.GENERATOR_MAX_COUNT, -1);

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		long start = System.currentTimeMillis();
		LOG.info("Generator: starting at " + sdf.format(start));
		LOG.info("Generator: Selecting best-scoring urls due for fetch.");
		LOG.info("Generator: filtering: " + filter);
		LOG.info("Generator: normalizing: " + norm);
		if (topN != Long.MAX_VALUE) {
			LOG.info("Generator: topN: " + topN);
		}
		if ("true".equals(getConf().get(GENERATE_MAX_PER_HOST_BY_IP))) {
			LOG.info("Generator: GENERATE_MAX_PER_HOST_BY_IP will be ignored, use partition.url.mode instead");
		}

		if (segCount == -1)
			segCount = 1;
		List<Path> generatedSegments = new ArrayList<Path>();
		int j = 0;// use for tablename
		String table = null;
		boolean isSmart = getConf().getBoolean("nutch.smart.is", false);
		int tableMax = getConf().getInt("generate.table.Max", 10);

		for (int i = 0; i < segCount; i++) {
			Path segment = null;

			if (isSmart) {
			} else {
				long segStart = System.currentTimeMillis();
				int cnt = 0;
				while (cnt == 0) {// 若无数据，换下一张有数据的表
					if (j++ == tableDepth)// depth张表都没数据？
					{
						if (generatedSegments.size() > 0)
							return generatedSegments.toArray(new Path[generatedSegments.size()]);
						else
							return null;
					}
					// 不能输出到相同的目录
					segment = new Path(segments, Generator.generateSegmentName());
					long begin = System.currentTimeMillis();
					table = "crawldb" + (tableMax + 1 - j);
					RunningJob r = generateJob(table, segment, reduceCnt, topN - cnt, filter, norm, force);
					Counter counter = r.getCounters().findCounter("Generator", "records");
					cnt += counter.getValue();
					LOG.info("Generator: " + segment + " records: " + cnt + " current table=" + table + " timeused="
							+ (System.currentTimeMillis() - begin) / 1000);
				}
				generatedSegments.add(segment);
				j--;

				GenerateInfo genInfo = GenerateInfos.getGenerateInfo();
				genInfo.start = segStart;
				genInfo.generate = cnt;
				genInfo.table = table;
				genInfo.end = System.currentTimeMillis();
				genInfo.endTime = sdf.format(genInfo.end);
			}
		}

		LOG.info(GenerateInfos.printString());
		long end = System.currentTimeMillis();
		LOG.info("Generator: finished at " + sdf.format(end) + ", elapsed: " + TimingUtil.elapsedTime(start, end));

		if (generatedSegments.size() > 0)
			return generatedSegments.toArray(new Path[generatedSegments.size()]);
		else
			return null;
	}

	private RunningJob generateJob(String table, Path segment, int reduceCnt, long topN, boolean filter, boolean norm,
			boolean force) throws IOException {
		LOG.info("Generator: segment=" + segment);

		JobConf job = new NutchJob(getConf());
		job.setJarByClass(GeneratorHbase.class);
		job.setJobName("generate: from " + table + " "
				+ (new SimpleDateFormat("MMdd HH:mm:ss")).format(System.currentTimeMillis()));
		// job.setLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 300000);

		if (reduceCnt == -1) {
			reduceCnt = job.getNumMapTasks(); // a partition per fetch task
		}
		if ("local".equals(job.get("mapred.job.tracker")) && reduceCnt != 1) {
			LOG.info("Generator: jobtracker is 'local', generating exactly one partition.");
			reduceCnt = 1;
		}
		// job.setLong(GENERATOR_CUR_TIME, curTime);
		// record real generation time
		long generateTime = System.currentTimeMillis();
		job.setLong(Nutch.GENERATE_TIME_KEY, generateTime);
		job.setLong(GENERATOR_TOP_N, topN);
		job.setBoolean(GENERATOR_FILTER, filter);
		job.setBoolean(GENERATOR_NORMALISE, norm);
		job.set(GENERATL_TABLE, table);
		job.setInt(GENERATL_REDUCECNT, reduceCnt);
		job.setInt("partition.url.seed", new Random().nextInt());

		job.setInputFormat(CodeInputFormat.class);
		job.setNumMapTasks(1);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setReducerClass(GenerateMark.class);
		job.setNumReduceTasks(reduceCnt);
		job.setOutputFormat(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(CrawlDatum.class);
		job.setOutputKeyComparatorClass(HashComparator.class);
		Path output = new Path(segment, CrawlDatum.GENERATE_DIR_NAME);
		FileOutputFormat.setOutputPath(job, output);

		RunningJob r = null;
		try {
			r = JobClient.runJob(job);
		} catch (IOException e) {
			throw e;
		}
		return r;
	}

	/**
	 * Generate a fetchlist from the crawldb.
	 */
	public static void main(String args[]) throws Exception {
		int res = ToolRunner.run(NutchConfiguration.create(), new GeneratorHbase(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		long topN = 80000;
		for (int i = 0; i < args.length; i++) {
			if ("-topN".equals(args[i])) {
				topN = Long.parseLong(args[i + 1]);
				i++;
			}
		}

		try {
			Path[] segs = generate(new Path("/data/segments"), 4, topN, false, false, false, 1, 1);
			if (segs == null)
				return -1;
		} catch (Exception e) {
			LOG.error("Generator: " + StringUtils.stringifyException(e));
			return -1;
		}
		return 0;
	}
}
