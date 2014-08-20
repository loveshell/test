package org.test.hadoop;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Tool;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.CrawlDb;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.util.HadoopFSUtil;
import org.apache.nutch.util.LockUtil;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.TimingUtil;
// Commons Logging imports
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HostDb extends Configured implements Tool {
	public static final Logger LOG = LoggerFactory.getLogger(HostDb.class);

	public static final String CURRENT_NAME = "current";
	public static final String LOCK_NAME = ".locked";

	public static final String KEY_SEED = "_seed_";
	public static final String KEY_FETCHE = "_fetch_";

	public static final byte HOST_BLACK = 1;
	public static final byte HOST_UNKNOW = 2;
	public static final byte HOST_WHITE = 3;

	public static float TOPIC_THRESHOLD = 0f;
	public static float TOPIC_WHITE = 0.9f;
	public static float TOPIC_BLACK = 0.05f;
	public static byte TOPIC_YES = 0X01;
	public static byte TOPIC_NO = 0X00;

	private static Map hosts = null;
	private static Configuration parseConfig = null;
	private static AtomicInteger dumping = new AtomicInteger(0);
	private static boolean needDump = true;
	private static AtomicInteger updateCnt = new AtomicInteger(0);

	// private static ThreadLocal dist = new ThreadLocal();

	/**
	 * 获取主机黑白名单类型
	 * 
	 * @param url
	 * @param conf
	 * @return
	 */
	public static byte getHostType(String url) {
		if (url != null) {
			String host = null;
			try {
				URL tmp = new URL(url);
				host = tmp.getHost();
			} catch (MalformedURLException e) {
				// e.printStackTrace();
			}
			if (host != null) {
				String key = host + "●";
				if (hosts == null) {
					try {
						loadHosts(parseConfig);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				if (hosts.containsKey(key)) {
					HostData data = (HostData) hosts.get(key);
					// if (LOG.isInfoEnabled())
					// LOG.info("getHostType: hit url=" + url);
					return getHostType(data);
				} else {
					// if (LOG.isInfoEnabled())
					// LOG.info("getHostType: 未找到url=" + url);
				}
			}
		}
		return HOST_UNKNOW;
	}

	/**
	 * 载入db到map
	 * 
	 * @param conf
	 * @return
	 * @throws IOException
	 */
	public static synchronized Map loadHosts(Configuration conf) throws IOException {
		if (hosts == null) {
			hosts = new HashMap();
			if (conf != null) {
				parseConfig = conf;

				FileSystem fs = FileSystem.get(conf);
				try {
					Path lock = new Path(conf.get("nutch.data.dir") + "/hostdump/" + conf.get("hostdump.lockname"));
					LockUtil.removeLockFile(fs, lock);
					LOG.info("loadHosts: 移除hostdump的锁=" + lock);
				} catch (Exception e1) {
					// e1.printStackTrace();
				}

				Path[] paths = DistributedCache.getLocalCacheFiles(conf);
				if (paths != null)
					LOG.info("distributedCache: cachefiles: " + Arrays.asList(paths));
				else
					LOG.warn("distributedCache: 本地没有缓存文件 ");

				if (paths != null && paths.length > 0) {
					for (Path path : paths) {
						LOG.info("hostdb: load hostdata from " + path);
						String line;
						String[] fields;
						BufferedReader joinReader = new BufferedReader(new FileReader(path.toString()));
						try {
							while ((line = joinReader.readLine()) != null) {
								fields = line.split(Character.toString(';'));
								if (fields.length < 6)
									continue;
								String key = fields[0] + "●";
								if (!StringUtil.isEmpty(fields[1]))
									key += fields[1];

								HostData value = new HostData();
								try {
									value.setSeed(Byte.valueOf(fields[2]));
									value.setTopicNum(Long.valueOf(fields[3]));
									value.setTotal(Long.valueOf(fields[4]));
									value.setUpdateInterval(Integer.valueOf(fields[5]));
								} catch (Exception e) {
									e.printStackTrace();
									continue;
								}

								hosts.put(key, value);
								// if (LOG.isInfoEnabled()) {
								// LOG.info(fields[0] + " " + fields[1] + " " +
								// value.toString());
								// }
							}
						} catch (Exception e) {
							LOG.error(e.getLocalizedMessage());
						} finally {
							joinReader.close();
						}
					}
				}
			} else {
				LOG.info("loadHosts: 没有初始化jobconfig，无法加载hostdb");
			}
			LOG.info("loadHosts:host list size=" + hosts.size());
		}
		return hosts;
	}

	private static byte getHostType(HostData data) {
		if (data != null) {
			if (data.getSeed() > 0)// 种子url
				return HOST_WHITE;
			else {
				long topic = data.getTopicNum();
				long total = data.getTotal();
				if (total >= 50) {// 数量太少无代表性
					float rate = topic / total;
					if (rate >= TOPIC_WHITE) {// 大于某值
						return HOST_WHITE;
					} else if (rate < TOPIC_BLACK)// 小于某值
						return HOST_BLACK;
				}
			}
		}

		return HOST_UNKNOW;
	}

	public static void main(String[] args) {
		// TODO
	}

	/**
	 * 使用现有的crawldb生成hostdb,在整个系统中只应初始化一次
	 * 
	 * @throws IOException
	 */
	public static void createHostdbFromCrawldb(Configuration config, Path crawlCurrent) throws IOException {
		long start = System.currentTimeMillis();
		FileSystem fs = FileSystem.get(config);
		Path hostDb = new Path(config.get("nutch.data.dir") + "/hostdb");
		Path newHostDb = new Path(hostDb, Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
		Path lock = new Path(hostDb, LOCK_NAME);
		LockUtil.createLockFile(fs, lock, false);

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		LOG.info("createhostdb: starting at " + sdf.format(start));
		LOG.info("createhostdb: crawlDb: " + crawlCurrent);
		LOG.info("createhostdb: hostDb: " + hostDb);

		JobConf createJob = new NutchJob(config);
		createJob.setJobName("createhostdb from " + crawlCurrent);

		FileInputFormat.addInputPath(createJob, crawlCurrent);
		createJob.setInputFormat(SequenceFileInputFormat.class);
		createJob.setMapperClass(CrawlTopicCountMapper.class);
		createJob.setMapOutputKeyClass(Text.class);
		createJob.setMapOutputValueClass(ByteWritable.class);

		createJob.setReducerClass(CrawlTopicCountReducer.class);
		createJob.setOutputFormat(MapFileOutputFormat.class);
		createJob.setOutputKeyClass(HostKey.class);
		createJob.setOutputValueClass(HostData.class);
		FileOutputFormat.setOutputPath(createJob, newHostDb);

		try {
			JobClient.runJob(createJob);
			install(createJob, hostDb);
		} catch (IOException e) {
			Path outPath = FileOutputFormat.getOutputPath(createJob);
			if (fs.exists(outPath))
				fs.delete(outPath, true);
			throw e;
		} finally {
			LockUtil.removeLockFile(fs, lock);
		}

		long end = System.currentTimeMillis();
		LOG.info("createhostdb: finished at " + sdf.format(end) + ", elapsed: " + TimingUtil.elapsedTime(start, end));
	}

	/**
	 * 种子url注入hostdb
	 * 
	 * @param config
	 * @param input
	 * @throws IOException
	 */
	public static void injectSeedHost(Configuration config, Path input) throws IOException {
		init(config);

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		long start = System.currentTimeMillis();
		Path hostDb = new Path(config.get("nutch.data.dir") + "/hostdb");
		Path tmpHostdb = new Path(hostDb, Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

		LOG.info("injecthostdb: starting at " + sdf.format(start));
		LOG.info("injecthostdb: crawlDb: " + input);
		LOG.info("injecthostdb: hostDb: " + hostDb);

		TokenCache.obtainTokensForNamenodes(new JobConf(config).getCredentials(), new Path[] { hostDb }, config);
		// 判断是否已有db，没有直接inject；
		if (hostDb.getFileSystem(config).exists(hostDb)) {
			injectMerge(config, input, hostDb);
		} else {
			inject(config, input, hostDb, tmpHostdb);
		}

		dumpJob(config);
		needDump = false;

		long end = System.currentTimeMillis();
		LOG.info("injecthostdb: finished at " + sdf.format(end) + ", elapsed: " + TimingUtil.elapsedTime(start, end));
	}

	private static void init(Configuration config) {
		// TOPIC_THRESHOLD = config.getFloat("generate.min.score", 5f);
		TOPIC_WHITE = config.getFloat("topic.relevance.whiterate", 0.9f);
		TOPIC_BLACK = config.getFloat("topic.relevance.blackrate", 0.05f);
	}

	private static void inject(Configuration config, Path input, Path hostDb, Path output) throws IOException {
		FileSystem fs = FileSystem.get(config);
		Path lock = new Path(hostDb, LOCK_NAME);
		LockUtil.createLockFile(fs, lock, false);

		JobConf job = getInjectJob(config, input, output);
		job.setOutputFormat(MapFileOutputFormat.class);
		try {
			JobClient.runJob(job);
			install(job, hostDb);
		} catch (IOException e) {
			Path outPath = FileOutputFormat.getOutputPath(job);
			if (fs.exists(outPath))
				fs.delete(outPath, true);
			throw e;
		} finally {
			LockUtil.removeLockFile(fs, lock);
		}
	}

	private static JobConf getInjectJob(Configuration config, Path input, Path output) throws IOException {
		JobConf job = new NutchJob(config);// 读取注入的crawldb生成hostdb
		job.setJobName("injecthostdb from " + input);

		FileInputFormat.addInputPath(job, input);
		job.setInputFormat(SequenceFileInputFormat.class);
		job.setMapperClass(CrawlTopicCountMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ByteWritable.class);

		job.setReducerClass(HostInjectReducer.class);
		job.setOutputFormat(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(HostKey.class);
		job.setOutputValueClass(HostData.class);
		FileOutputFormat.setOutputPath(job, output);
		job.setNumReduceTasks(1);

		return job;
	}

	/**
	 * 种子url注入hostdb
	 * 
	 * @param config
	 * @param input
	 * @throws IOException
	 */
	private static void injectMerge(Configuration config, Path input, Path hostDb) throws IOException {
		FileSystem fs = FileSystem.get(config);
		Path tempDir = new Path(config.get("mapred.temp.dir", ".") + "/injecthostdb-temp-"
				+ Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

		JobConf job = getInjectJob(config, input, tempDir);
		try {
			JobClient.runJob(job);
		} catch (IOException e) {
			Path outPath = FileOutputFormat.getOutputPath(job);
			if (fs.exists(outPath))
				fs.delete(outPath, true);
			throw e;
		}

		mergeSeedHost(config, hostDb, tempDir);
		fs.delete(tempDir, true);
	}

	/**
	 * inject 的host和现有的hostdb合并
	 * 
	 * @param config
	 * @param hostDb
	 * @param input
	 * @throws IOException
	 */
	private static void mergeSeedHost(Configuration config, Path hostDb, Path input) throws IOException {
		FileSystem fs = FileSystem.get(config);
		Path lock = new Path(hostDb, LOCK_NAME);
		LockUtil.createLockFile(fs, lock, false);

		Path tmpHostdb = new Path(hostDb, Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
		Path currentHostdb = new Path(hostDb, CURRENT_NAME);
		// merge with existing host db
		LOG.info("injecthostdb: Merging from " + input + " and " + currentHostdb);

		JobConf job = new NutchJob(config);// 合并现有的hostdb和新注入的
		job.setJobName("injecthostdb merge " + hostDb);
		FileInputFormat.addInputPath(job, input);
		if (FileSystem.get(job).exists(currentHostdb)) {
			FileInputFormat.addInputPath(job, currentHostdb);
		}
		job.setInputFormat(SequenceFileInputFormat.class);

		job.setReducerClass(HostMergeReducer.class);
		job.setOutputFormat(MapFileOutputFormat.class);
		job.setOutputKeyClass(HostKey.class);
		job.setOutputValueClass(HostData.class);
		FileOutputFormat.setOutputPath(job, tmpHostdb);
		job.setNumReduceTasks(1);

		try {
			JobClient.runJob(job);
			install(job, hostDb);
		} catch (IOException e) {
			Path outPath = FileOutputFormat.getOutputPath(job);
			if (fs.exists(outPath))
				fs.delete(outPath, true);
			throw e;
		} finally {
			fs.delete(input, true);
			LockUtil.removeLockFile(fs, lock);
		}
	}

	private static void dumpJob(Configuration config) throws IOException {
		Path hostdb = new Path(config.get("nutch.data.dir") + "/hostdb");
		Path dbCurrent = new Path(hostdb, CURRENT_NAME);
		Path dump = new Path(config.get("nutch.data.dir") + "/hostdump");
		Path dumpTmp = new Path(config.get("nutch.data.dir") + "/hostdump-"
				+ Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

		FileSystem fs = FileSystem.get(config);
		if (fs.exists(dbCurrent)) {
			LOG.info("hostdb dump: starting");

			Path lock = new Path(dump, LOCK_NAME);
			Path lockHostdb = new Path(hostdb, LOCK_NAME);
			LockUtil.createLockFile(fs, lock, false);
			LockUtil.createLockFile(fs, lockHostdb, false);

			JobConf job = new NutchJob(config);// 导出hostdb
			job.setJobName("hostdb dump");

			FileInputFormat.addInputPath(job, dbCurrent);
			job.setInputFormat(SequenceFileInputFormat.class);

			job.setOutputFormat(CsvOutputFormat.class);
			job.setOutputKeyClass(HostKey.class);
			job.setOutputValueClass(HostData.class);
			FileOutputFormat.setOutputPath(job, dumpTmp);
			job.setNumReduceTasks(1);

			try {
				JobClient.runJob(job);
				dumpInstall(job, dump);
			} catch (IOException e) {
				Path outPath = FileOutputFormat.getOutputPath(job);
				if (fs.exists(outPath))
					fs.delete(outPath, true);

				throw e;
			} finally {
				LockUtil.removeLockFile(fs, lock);
				LockUtil.removeLockFile(fs, lockHostdb);
			}
			LOG.info("hostdb dump: done");
		} else {
			LOG.info("hostdb dump: not exist " + dbCurrent);
		}
	}

	private static void dumpInstall(JobConf job, Path dump) throws IOException {
		Path newDir = FileOutputFormat.getOutputPath(job);
		FileSystem fs = new JobClient(job).getFs();
		Path old = new Path(dump, "old");
		Path current = new Path(dump, CURRENT_NAME);
		if (fs.exists(current)) {
			if (fs.exists(old))
				fs.delete(old, true);
			fs.rename(current, old);
		}
		fs.mkdirs(dump);
		fs.rename(newDir, current);
	}

	/**
	 * 注意多线程
	 * 
	 * @param conf
	 * @throws IOException
	 */
	public static void distributeHostdb(Configuration conf) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path dump = new Path(conf.get("nutch.data.dir") + "/hostdump");
		String lockName = ".locked-" + Thread.currentThread().getName();
		conf.set("hostdump.lockname", lockName);

		if (dumping.getAndIncrement() == 0) {
			if (needDump && !hasOtherLock(conf, lockName)) {
				try {
					dumpJob(conf);// 没有更新就不要dump，有其他线程锁了也不要更新

					needDump = false;
					// dist = new ThreadLocal();// 清空
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			dumping.decrementAndGet();
		} else {
			dumping.decrementAndGet();
			LOG.info("有其他线程在dumping");
			while (dumping.get() != 0) {
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			LOG.info("其他线程dumping结束，直接利用。");
		}

		lockDump(conf, lockName);

		// Boolean disted = (Boolean) dist.get();
		// if (disted == null) {
		Path current = new Path(dump, HostDb.CURRENT_NAME);
		FileStatus[] paths = fs.listStatus(current);
		// FileStatus[] paths =
		// fs.listStatus(dump,HadoopFSUtil.getPassDirectoriesFilter(fs));
		Path[] dirs = HadoopFSUtil.getPaths(paths);
		if (dirs != null && dirs.length > 0) {
			for (Path path : dirs) {
				DistributedCache.addCacheFile(path.toUri(), conf);// 分发缓存hostdb
				LOG.info("hostdb DistributedCache: " + path.toString());
			}
			// dist.set(Boolean.TRUE);
		} else
			LOG.info("hostdb DistributedCache: 不存在文件 " + current.toString());
		// } else {
		// LOG.info("hostdb DistributedCache: 直接利用上次的缓存");
	}

	// }

	/**
	 * 
	 * @param conf
	 * @throws Exception
	 */
	private static boolean lockDump(Configuration conf, String lockName) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path dump = new Path(conf.get("nutch.data.dir") + "/hostdump");
		Path lock = new Path(dump, lockName);
		try {
			LockUtil.createLockFile(fs, lock, false);// 先锁定
			return true;
		} catch (Exception e) {// 已经被其他线程缓存到客户端了
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * 代价小
	 * 
	 * @param conf
	 * @param lockName
	 * @return
	 * @throws Exception
	 */
	private synchronized static boolean hasOtherLock(Configuration conf, String lockName) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path dump = new Path(conf.get("nutch.data.dir") + "/hostdump");
		FileStatus[] paths = fs.listStatus(dump);
		Path[] dirs = HadoopFSUtil.getPaths(paths);
		if (dirs != null && dirs.length > 0) {
			for (Path path : dirs) {
				if (path.getName().indexOf(lockName) > 0)
					continue;
				if (path.getName().indexOf(".locked-") > 0) {
					return true;
				}
			}

		}
		return false;
	}

	/**
	 * 更新hostdb
	 * 
	 * @param crawlDb
	 * @throws IOException
	 */
	public static void updateHostDb(Configuration config, Path[] inputs) throws IOException {
		long start = System.currentTimeMillis();
		FileSystem fs = FileSystem.get(config);
		Path hostDb = new Path(config.get("nutch.data.dir") + "/hostdb");
		Path tempDirHost = new Path(config.get("mapred.temp.dir", ".") + "/updatehostdb-temp-"
				+ Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		LOG.info("updateHostDb: count crawldb for host topic.");
		LOG.info("updateHostDb: starting at " + sdf.format(start));
		LOG.info("updateHostDb: hostDb: " + hostDb);
		LOG.info("updateHostDb: 新增的crawlDb: " + Arrays.asList(inputs));

		JobConf countjob = new NutchJob(config);
		countjob.setJobName("updateHostDb count crawldb for topic");

		FileInputFormat.setInputPaths(countjob, inputs);
		countjob.setInputFormat(SequenceFileInputFormat.class);
		countjob.setMapperClass(CrawlTopicCountMapper.class);
		countjob.setMapOutputKeyClass(Text.class);
		countjob.setMapOutputValueClass(ByteWritable.class);

		countjob.setReducerClass(CrawlTopicCountReducer.class);
		countjob.setOutputFormat(SequenceFileOutputFormat.class);
		countjob.setOutputKeyClass(HostKey.class);
		countjob.setOutputValueClass(HostData.class);
		FileOutputFormat.setOutputPath(countjob, tempDirHost);
		countjob.setNumReduceTasks(1);

		try {
			JobClient.runJob(countjob);
		} catch (IOException e) {
			Path outPath = FileOutputFormat.getOutputPath(countjob);
			if (fs.exists(outPath))
				fs.delete(outPath, true);
			throw e;
		}

		// merge with existing host db
		LOG.info("updateHostDb: Merging from new crawldb into hostdb.");
		Path lock = new Path(hostDb, LOCK_NAME);
		LockUtil.createLockFile(fs, lock, false);

		Path newHostDb = new Path(hostDb, Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
		Path currentHostdb = new Path(hostDb, CURRENT_NAME);

		JobConf mergeJob = new NutchJob(config);
		mergeJob.setJobName("updateHostDb merge " + hostDb);

		FileInputFormat.addInputPath(mergeJob, tempDirHost);
		if (FileSystem.get(mergeJob).exists(currentHostdb)) {
			FileInputFormat.addInputPath(mergeJob, currentHostdb);
		}
		mergeJob.setInputFormat(SequenceFileInputFormat.class);

		mergeJob.setReducerClass(HostMergeReducer.class);
		mergeJob.setOutputFormat(MapFileOutputFormat.class);
		mergeJob.setOutputKeyClass(HostKey.class);
		mergeJob.setOutputValueClass(HostData.class);
		FileOutputFormat.setOutputPath(mergeJob, newHostDb);
		mergeJob.setNumReduceTasks(1);

		try {
			JobClient.runJob(mergeJob);
			install(mergeJob, hostDb);
		} catch (IOException e) {
			Path outPath = FileOutputFormat.getOutputPath(mergeJob);
			if (fs.exists(outPath))
				fs.delete(outPath, true);
			throw e;
		} finally {
			LockUtil.removeLockFile(fs, lock);
		}

		if (updateCnt.incrementAndGet() % 5 == 0) {
			needDump = true;
			updateCnt.set(0);
		}
		// clean up
		fs.delete(tempDirHost, true);

		long end = System.currentTimeMillis();
		LOG.info("updateHostDb: finished at " + sdf.format(end) + ", elapsed: " + TimingUtil.elapsedTime(start, end));
	}

	/**
	 * 老的删除，新的改名到current下
	 * 
	 * @param job
	 * @param hostDb
	 * @throws IOException
	 */
	private static void install(JobConf job, Path hostDb) throws IOException {
		Path newCrawlDb = FileOutputFormat.getOutputPath(job);
		FileSystem fs = new JobClient(job).getFs();
		Path old = new Path(hostDb, "old");
		Path current = new Path(hostDb, CURRENT_NAME);
		if (fs.exists(current)) {
			if (fs.exists(old))
				fs.delete(old, true);
			fs.rename(current, old);
		}
		fs.mkdirs(hostDb);
		fs.rename(newCrawlDb, current);
		Path lock = new Path(hostDb, LOCK_NAME);
		LockUtil.removeLockFile(fs, lock);

		LOG.info("installHostDb: Merging hostdb " + newCrawlDb);
	}

	public static class CsvOutputFormat extends FileOutputFormat<HostKey, HostData> {
		protected static class LineRecordWriter implements RecordWriter<HostKey, HostData> {
			private DataOutputStream out;

			public LineRecordWriter(DataOutputStream out) {
				this.out = out;
			}

			public synchronized void write(HostKey key, HostData value) throws IOException {
				out.writeBytes(key.getHost().toString());
				out.writeByte(';');
				out.writeBytes(key.getChannel().toString());
				out.writeByte(';');

				out.writeBytes(Byte.toString(value.getSeed()));
				out.writeByte(';');
				out.writeBytes(Long.toString(value.getTopicNum()));
				out.writeByte(';');
				out.writeBytes(Long.toString(value.getTotal()));
				out.writeByte(';');
				out.writeBytes(Integer.toString(value.getUpdateInterval()));

				out.writeByte('\n');
			}

			public synchronized void close(Reporter reporter) throws IOException {
				out.close();
			}
		}

		public RecordWriter<HostKey, HostData> getRecordWriter(FileSystem fs, JobConf job, String name,
				Progressable progress) throws IOException {
			Path dir = FileOutputFormat.getOutputPath(job);
			DataOutputStream fileOut = fs.create(new Path(dir, name), progress);
			return new LineRecordWriter(fileOut);
		}
	}

	/**
	 * 输出<host,是否主题相关>
	 * 
	 * @author xxhuang
	 * 
	 */
	public static class CrawlTopicCountMapper extends MapReduceBase implements
			Mapper<Text, CrawlDatum, Text, ByteWritable> {
		public static final String URL_FILTERING = "crawldb.url.filters";
		public static final String URL_NORMALIZING = "crawldb.url.normalizers";
		public static final String URL_NORMALIZING_SCOPE = "crawldb.url.normalizers.scope";

		private boolean urlFiltering;
		private boolean urlNormalizers;
		private boolean url404Purging;
		private URLFilters filters;
		private URLNormalizers normalizers;
		private String scope;

		public void configure(JobConf job) {
			urlFiltering = job.getBoolean(URL_FILTERING, false);
			urlNormalizers = job.getBoolean(URL_NORMALIZING, false);
			url404Purging = job.getBoolean(CrawlDb.CRAWLDB_PURGE_404, false);

			if (urlFiltering) {
				filters = new URLFilters(job);
			}
			if (urlNormalizers) {
				scope = job.get(URL_NORMALIZING_SCOPE, URLNormalizers.SCOPE_CRAWLDB);
				normalizers = new URLNormalizers(job, scope);
			}
		}

		public void map(Text key, CrawlDatum value, OutputCollector<Text, ByteWritable> output, Reporter reporter)
				throws IOException {
			String url = key.toString();
			// https://issues.apache.org/jira/browse/NUTCH-1101 check status
			// first,
			// cheaper than normalizing or filtering
			if (url404Purging && CrawlDatum.STATUS_DB_GONE == value.getStatus()) {
				url = null;
			}
			if (url != null && urlNormalizers) {
				try {
					url = normalizers.normalize(url, scope); // normalize the
																// url
				} catch (Exception e) {
					LOG.warn("Skipping " + url + ":" + e);
					url = null;
				}
			}
			if (url != null && urlFiltering) {
				try {
					url = filters.filter(url); // filter the url
				} catch (Exception e) {
					LOG.warn("Skipping " + url + ":" + e);
					url = null;
				}
			}

			if (url != null) {
				try {
					URL tmp = new URL(url);
					String host = tmp.getHost();
					if (!StringUtil.isEmpty(host)) {
						if (value.getScore() > TOPIC_THRESHOLD)
							output.collect(new Text(host), new ByteWritable(TOPIC_YES));
						else
							output.collect(new Text(host), new ByteWritable(TOPIC_NO));
					}
				} catch (MalformedURLException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * 输出<<url,channel> hostdata>
	 * 
	 * @author xxhuang
	 * 
	 */
	public static class CrawlTopicCountReducer extends MapReduceBase implements
			Reducer<Text, ByteWritable, HostKey, HostData> {
		public void reduce(Text key, Iterator<ByteWritable> values, OutputCollector<HostKey, HostData> output,
				Reporter reporter) throws IOException {
			HostKey hostKey = new HostKey();
			hostKey.setHost(key);
			hostKey.setChannel(new Text());

			HostData hostData = new HostData();
			org.apache.hadoop.io.MapWritable meta = new org.apache.hadoop.io.MapWritable();
			meta.put(new Text(KEY_FETCHE), new Text("1"));
			hostData.setMetaData(meta);
			long total = 0;
			long topic = 0;
			while (values.hasNext()) {
				ByteWritable value = values.next();
				if (TOPIC_YES == value.get()) {
					topic++;
				}
				total++;
			}
			hostData.setTopicNum(topic);
			hostData.setTotal(total);

			output.collect(hostKey, hostData);
		}
	}

	public static class HostInjectReducer extends MapReduceBase implements
			Reducer<Text, ByteWritable, HostKey, HostData> {

		public void reduce(Text key, Iterator<ByteWritable> values, OutputCollector<HostKey, HostData> output,
				Reporter reporter) throws IOException {
			HostKey hostKey = new HostKey();
			hostKey.setHost(key);
			hostKey.setChannel(new Text());

			// int total = Iterators.size(values);
			HostData hostData = new HostData();
			hostData.setSeed(HOST_WHITE);
			// hostData.setTopicNum(total);
			// hostData.setTotal(total);
			org.apache.hadoop.io.MapWritable meta = new org.apache.hadoop.io.MapWritable();
			meta.put(new Text(KEY_SEED), new Text("1"));
			hostData.setMetaData(meta);

			output.collect(hostKey, hostData);
			// if (LOG.isInfoEnabled()) {
			// LOG.info("injectHostDb: url=" + key + " seed=" +
			// hostData.getSeed());
			// }
		}
	}

	/** Combine multiple new entries for a host. */
	public static class HostMergeReducer extends MapReduceBase implements Reducer<HostKey, HostData, HostKey, HostData> {

		public void reduce(HostKey key, Iterator<HostData> values, OutputCollector<HostKey, HostData> output,
				Reporter reporter) throws IOException {
			long topic = 0;
			long total = 0;
			HostData data = new HostData();
			String action = "";
			while (values.hasNext()) {
				HostData val = values.next();
				topic += val.getTopicNum();
				total += val.getTotal();
				Text seed = (Text) val.getMetaData().get(new Text(KEY_SEED));
				Text fetch = (Text) val.getMetaData().get(new Text(KEY_FETCHE));
				if (seed != null && !StringUtil.isEmpty(seed.toString())) {// new
					data.setSeed(val.getSeed());
					action = "注入过程";
				} else if (fetch != null && !StringUtil.isEmpty(fetch.toString())) {
					action = "更新过程";
				} else {// old
					if (val.getSeed() > 0)
						data.setSeed(val.getSeed());
					data.setUpdateInterval(val.getUpdateInterval());
					// data.setMetaData(val.getMetaData());
				}
			}
			data.setTopicNum(topic);
			data.setTotal(total);
			output.collect(key, data);
			// if (LOG.isInfoEnabled()) {
			// LOG.info("mergeHostDb:" + action + " url=" + key.getHost() +
			// " channel=" + key.getChannel());
			// LOG.info(data.toString());
			// }
		}
	}

	public static class HostKey implements WritableComparable<HostKey> {
		private Text host = new Text();
		private Text channel = new Text();

		public Text getHost() {
			return host;
		}

		public void setHost(Text host) {
			this.host = host;
		}

		public Text getChannel() {
			return channel;
		}

		public void setChannel(Text channel) {
			this.channel = channel;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			host.write(out);
			channel.write(out);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			host.readFields(in);
			channel.readFields(in);
		}

		@Override
		public int compareTo(HostKey that) {
			if (that.host != this.host)
				return this.host.compareTo(that.host);
			if (that.channel != this.channel)
				return this.channel.compareTo(that.channel);

			return 0;
		}

		@Override
		public int hashCode() {
			String tmp = "●";
			if (host != null)
				tmp += host.toString();
			tmp += "●";
			if (channel != null)
				tmp += channel.toString();
			return tmp.hashCode();
		}
	}

	public static class HostData implements WritableComparable<HostData> {
		private byte seed;// 种子标志
		private long topicNum;// 主题相关数
		private long total;// 总发现数
		private int updateInterval;// 跟新间隔 毫秒
		private org.apache.hadoop.io.MapWritable metaData;

		public void setMetaData(org.apache.hadoop.io.MapWritable mapWritable) {
			this.metaData = new org.apache.hadoop.io.MapWritable(mapWritable);
		}

		public void putAllMetaData(HostData other) {
			for (Entry<Writable, Writable> e : other.getMetaData().entrySet()) {
				getMetaData().put(e.getKey(), e.getValue());
			}
		}

		public org.apache.hadoop.io.MapWritable getMetaData() {
			if (this.metaData == null)
				this.metaData = new org.apache.hadoop.io.MapWritable();
			return this.metaData;
		}

		public byte getSeed() {
			return seed;
		}

		public void setSeed(byte seed) {
			this.seed = seed;
		}

		public int getUpdateInterval() {
			return updateInterval;
		}

		public void setUpdateInterval(int updateInterval) {
			this.updateInterval = updateInterval;
		}

		public long getTotal() {
			return total;
		}

		public void setTotal(long total) {
			this.total = total;
		}

		public long getTopicNum() {
			return topicNum;
		}

		public void setTopicNum(long topicNum) {
			this.topicNum = topicNum;
		}

		public String toString() {
			StringBuilder buf = new StringBuilder();
			buf.append("类型:" + getHostTypeName(this) + " ");
			buf.append("seed:" + getSeed() + " ");
			buf.append("topicNum:" + topicNum + " ");
			buf.append("total:" + total + " ");
			buf.append("interval:" + updateInterval + "s ");
			buf.append("Meta:");
			if (metaData != null) {
				for (Entry<Writable, Writable> e : metaData.entrySet()) {
					buf.append(e.getKey());
					buf.append(": ");
					buf.append(e.getValue());
				}
			}
			return buf.toString();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeByte(seed);
			out.writeLong(topicNum);
			out.writeLong(total);
			out.writeInt(updateInterval);

			if (metaData != null && metaData.size() > 0) {
				out.writeBoolean(true);
				metaData.write(out);
			} else {
				out.writeBoolean(false);
			}
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			seed = in.readByte();
			topicNum = in.readLong();
			total = in.readLong();
			updateInterval = in.readInt();

			boolean hasMetadata = false;
			if (in.readBoolean()) {
				hasMetadata = true;
				metaData = new org.apache.hadoop.io.MapWritable();
				metaData.readFields(in);
			}
			if (hasMetadata == false)
				metaData = null;
		}

		/** Sort by decreasing score. */
		public int compareTo(HostData that) {
			if (that.seed != this.seed)
				return (that.seed - this.seed) > 0 ? 1 : -1;
			if (that.topicNum != this.topicNum)
				return (that.topicNum / that.total - this.topicNum / this.total) > 0 ? 1 : -1;

			return 0;
		}

	}

	public HostDb(Configuration conf) {
		setConf(conf);
	}

	@Override
	public int run(String[] args) throws Exception {
		return 0;
	}

	private static String getHostTypeName(HostData data) {
		byte value = getHostType(data);
		if (HOST_BLACK == value)
			return "黑";
		if (HOST_WHITE == value)
			return "白";
		return "未知";
	}
}
