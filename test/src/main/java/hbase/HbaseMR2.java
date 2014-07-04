package hbase;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.CrawlDatum;

public class HbaseMR2 extends Configured implements Tool {
	public static Configuration conf = HBaseConfiguration.create();
	static final Log LOG = LogFactory.getLog(HbaseMR2.class);

	/**
	 * 自定义文件写表
	 * 
	 * @author xxhuang
	 */
	public static class FileToTableMap implements org.apache.hadoop.mapred.Mapper<Text, CrawlDatum, Text, CrawlDatum> {
		Configuration configuration = null;
		HConnection connection = null;
		HTableInterface idxTable = null;
		private boolean wal = false;
		static long count = 0;
		private Map tableMap = new HashMap();
		private long mapStart = 0;

		public void configure(JobConf job) {
			mapStart = System.currentTimeMillis();
			configuration = job;
			// System.out.println(configuration.get("hbase.zookeeper.quorum"));
			// System.out.println(conf.get("hbase.zookeeper.quorum"));
			try {
				connection = HConnectionManager.createConnection(configuration);

				idxTable = connection.getTable("crawldbIdx");
				idxTable.setAutoFlush(false, true);
				idxTable.setWriteBufferSize(12 * 1024 * 1024);
				wal = false;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		public void close() throws IOException {
			idxTable.flushCommits();
			idxTable.close();
			commitTable();
			closeTable();
			connection.close();
			long mapend = System.currentTimeMillis();
			System.out.println("hdfstotable: 这个map耗时毫秒=" + (mapend - mapStart));
			System.out.println("hdfstotable: 这个map共处理记录条数是=" + count + "记录/每秒=" + count * 1000 / (mapend - mapStart));
			System.out.println("hdfstotable: 这个map启动时间=" + getDate(mapStart) + "结束时间=" + getDate(mapend));
		}

		public void map(Text key, CrawlDatum value, OutputCollector<Text, CrawlDatum> output, Reporter reporter)
				throws IOException {
			++count;
			// long tmp = (++count) % 5;
			value.setScore(1);
			// if (tmp == 0)
			// value.setScore(5);

			String shortKey = ShortUrlGenerator.shortBy62(key.toString());
			idxTable.put(getIdxPut(shortKey, value));
			insertUrls(shortKey, key.toString(), value);

			if ((count % 10000) == 0) {
				reporter.setStatus(count + " urls done!");
				reporter.progress();
				System.out.println("hdfstotable: " + count + " urls done!");
			}
			if ((count % 200000) == 0) {
				idxTable.flushCommits();
				commitTable();
			}
			reporter.incrCounter("hdfstotable", "urlNum", 1);
		}

		private void commitTable() throws IOException {
			for (Object o : tableMap.values()) {
				((HTableInterface) o).flushCommits();
			}
		}

		private void closeTable() throws IOException {
			for (Object o : tableMap.values()) {
				((HTableInterface) o).close();
			}
		}

		private Put getIdxPut(String shortKey, CrawlDatum value) throws IOException {
			Put put = new Put(Bytes.toBytes(shortKey));
			put.add(Bytes.toBytes("cf1"), Bytes.toBytes("score"), Bytes.toBytes(value.getScore()));

			if (!wal) {
				put.setDurability(Durability.SKIP_WAL);
			}
			return put;
		}

		private void insertUrls(String shortKey, String url, CrawlDatum value) throws IOException {
			HTableInterface htable = getHtable(Float.valueOf(value.getScore()).longValue());
			Put put = new Put(Bytes.toBytes(shortKey));

			put.add(Bytes.toBytes("cf1"), Bytes.toBytes("url"), Bytes.toBytes(url));
			put.add(Bytes.toBytes("cf1"), Bytes.toBytes("Status"), new byte[] { value.getStatus() });
			put.add(Bytes.toBytes("cf1"), Bytes.toBytes("Fetchtime"), Bytes.toBytes(value.getFetchTime()));
			// put.add(Bytes.toBytes("cf1"), Bytes.toBytes("Modifiedtime"),
			// Bytes.toBytes(value.getModifiedTime()));
			put.add(Bytes.toBytes("cf1"), Bytes.toBytes("RetriesSinceFetch"),
					new byte[] { value.getRetriesSinceFetch() });
			// put.add(Bytes.toBytes("cf1"), Bytes.toBytes("FetchInterval"),
			// Bytes.toBytes(value.getFetchInterval()));
			if (value.getSignature() != null && value.getSignature().length != 0)
				put.add(Bytes.toBytes("cf1"), Bytes.toBytes("Signature"), value.getSignature());

			// org.apache.hadoop.io.MapWritable metaData = value.getMetaData();
			// if (metaData != null) {
			// for (Entry<Writable, Writable> e : metaData.entrySet()) {
			// put.add(Bytes.toBytes("cf1"),
			// Bytes.toBytes(e.getKey().toString()),
			// Bytes.toBytes(e.getValue().toString()));
			// }
			// }

			if (!wal) {
				put.setDurability(Durability.SKIP_WAL);
			}

			htable.put(put);
		}

		private HTableInterface getHtable(long score) throws IOException {
			Long lscore = Long.valueOf(score);
			if (!tableMap.containsKey(lscore)) {
				tableMap.put(lscore, connection.getTable("crawldb" + score));
			}

			return (HTableInterface) tableMap.get(lscore);
		}
	}

	public int run(String[] args) throws Exception {
		Path current = new Path("/nutch/entitydata/crawldb/current");
		JobConf job = new JobConf(conf);
		job.setJobName("crawldb: hdfs to htable");
		job.setJarByClass(HbaseMR2.class);

		job.setMapperClass(FileToTableMap.class);
		job.setInputFormat(SequenceFileInputFormat.class);
		FileInputFormat.addInputPath(job, current);

		job.setNumReduceTasks(0);
		job.setOutputFormat(org.apache.hadoop.mapred.lib.NullOutputFormat.class);

		long start = System.currentTimeMillis();
		JobClient.runJob(job);
		System.out.println("hdfstotable:共耗时=" + (System.currentTimeMillis() - start));
		System.out.println("hdfstotable: 这个job启动时间=" + getDate(start) + "结束时间=" + getDate());
		return 0;
	}

	private static String getDate() {
		return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(System.currentTimeMillis()));
	}

	private static String getDate(long time) {
		return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(time));
	}

	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		int res = 1;
		try {
			res = ToolRunner.run(conf, new HbaseMR2(), otherArgs);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(res);
	}
}