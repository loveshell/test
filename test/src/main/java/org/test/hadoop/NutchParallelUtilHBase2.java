/**
 * 
 */
package org.test.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.nutch.crawl.CrawlDb;
import org.apache.nutch.fetcher.Fetcher;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.ParseSegment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author xxhuang
 * 
 */
public class NutchParallelUtilHBase2 {
	// Runnable不会返回结果，并且无法抛出经过检查的异常而Callable又返回结果，而且当获取返回结果时可能会抛出异常。
	// Callable中的call()方法类似Runnable的run()方法，区别同样是有返回值，后者没有。
	// 如果Future的返回尚未完成，则get（）方法会阻塞等待，直到Future完成返回，可以通过调用isDone（）方法判断Future是否完成了返回。
	// lock.lockInterruptibly();//获取响应中断锁
	// reader.interrupt(); //尝试中断读线程
	// while(!fs.isDone);//Future返回如果没有完成，则一直循环等待，直到Future返回完成
	public static Logger LOG = LoggerFactory.getLogger(NutchParallelUtilHBase2.class);
	protected static int genMax = 20;
	protected static int genNum = 2;
	protected static int genFirst = 4;
	protected static int updateThreshold = 2;
	protected static int fetchMap = 4;
	protected static int tableDepth = 10;
	protected static float minCrawlDelay = 0.0f;
	// 容易死锁啊
	protected static BlockingQueue<Path> generatedSegments = new ArrayBlockingQueue<Path>(genMax, true);
	protected static BlockingQueue<Path> fetchedSegments = new ArrayBlockingQueue<Path>(genMax * 4, true);
	protected static BlockingQueue<Path> parsedSegments = new ArrayBlockingQueue<Path>(genMax * 4, true);

	protected static AtomicInteger gNullCount = new AtomicInteger(0);
	protected static AtomicInteger genSegCount = new AtomicInteger(0);
	protected static AtomicInteger fetchSegCount = new AtomicInteger(0);
	protected static AtomicInteger fetching = new AtomicInteger(0);
	protected static AtomicInteger parSegCount = new AtomicInteger(0);
	protected static List<Path> toUpdate = new ArrayList<Path>();
	protected static boolean updateNeedGen = false;// 更新后，值改变
	static boolean fetchAhead = false;// 提前启动fetchjob开关
	static AtomicBoolean fetchJobing = new AtomicBoolean(false);// fetchjob争用标志
	Path currentFetch = null;//

	protected static JobConf job;
	protected static GeneratorRedHbase generator = null;
	protected static Fetcher fetcher = null;
	protected static ParseSegment parser = null;
	protected static CrawlDb crawlDbTool = null;
	protected static Path rootSeg = null;
	protected static Path crawldb = null;
	protected static boolean filter = true;
	protected static boolean normalise = true;
	protected static long topN = 1000;
	protected static int threads = 5;

	public static void init(JobConf job, GeneratorRedHbase generator, Fetcher fetcher, ParseSegment parseSegment,
			CrawlDb crawlDbTool, int fetchThreads, int generate, int uThreshold) {
		init(job, generator, fetcher, parseSegment, crawlDbTool, fetchThreads, generate, uThreshold, 4);
	}

	public static void init(JobConf job, GeneratorRedHbase generator, Fetcher fetcher, ParseSegment parseSegment,
			CrawlDb crawlDbTool, int fetchThreads, int generate, int uThreshold, int fetchMap) {
		job.setBoolean("fetcher.parse", false);
		filter = job.getBoolean(GeneratorRedHbase.GENERATOR_FILTER, true);
		normalise = job.getBoolean(GeneratorRedHbase.GENERATOR_NORMALISE, true);
		threads = fetchThreads;

		genFirst = generate;
		if (genFirst > genMax)
			genFirst = genMax;
		genNum = genFirst;
		updateThreshold = uThreshold;
		tableDepth = job.getInt("generate.table.depth", 10);
		minCrawlDelay = job.getFloat("fetcher.server.min.delay", 0.0f);
		fetchAhead = job.getBoolean("fetcher.nextstart.ahead", false);
		NutchParallelUtilHBase2.fetchMap = fetchMap;
		NutchParallelUtilHBase2.job = job;
		NutchParallelUtilHBase2.generator = generator;
		NutchParallelUtilHBase2.fetcher = fetcher;
		NutchParallelUtilHBase2.parser = parseSegment;
		NutchParallelUtilHBase2.crawlDbTool = crawlDbTool;
		NutchParallelUtilHBase2.fetcher.getConf().setInt("fetchMap", fetchMap);
	}

	public void process(Path crawlDb, Path segments, int numLists, long topN, boolean force) throws Exception {
		NutchParallelUtilHBase2.topN = topN;
		NutchParallelUtilHBase2.rootSeg = segments;
		NutchParallelUtilHBase2.crawldb = crawlDb;

		// generate new segment
		generateUpdate();
		fetch(1); // fetch it
		if (fetchAhead)
			fetch(2);
		parse();

		check();
	}

	protected void check() throws Exception {
		long cnt = 0;
		while (true) {
			if (gNullCount.get() > 0 && genNum == genFirst)
				throw new Exception("第一次没有generate到数据，程序退出.................................");
			if (gNullCount.get() > 0) {
				if (cnt++ == 360)
					throw new Exception("over 60 minutes 都没有url被generate，程序退出.................................");
			} else {
				cnt = 0;
			}

			Thread.sleep(10000);
		}
	}

	protected void generateUpdate() throws IOException {
		Thread t = new Thread() {
			public void run() {
				Thread.currentThread().setName("generateUpdate线程");
				LOG.info(Thread.currentThread() + ": 线程启动");
				while (true) {
					if (shouldGenerate()) {
						LOG.info("generator: segment个数不足，开始generate。");
						try {
							generate(genNum);
						} catch (Exception e) {
							e.printStackTrace();
						}
						LOG.info("generator: 完成generate。");
					}
					if (shouldUpdate()) {
						LOG.info("updater: 开始update crawldb。");
						try {
							update(true, true);
						} catch (Exception e) {
							e.printStackTrace();
						}
						LOG.info("updater: 完成update crawldb。");
					}
					try {
						Thread.sleep(8000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		};
		t.setDaemon(true);
		t.start();
	}

	/**
	 * 不一定非常精确
	 * 
	 * @return
	 */
	private boolean shouldGenerate() {
		if (updateNeedGen && genSegCount.get() < genNum)// 更新了url库,并且已经generate的数目小于临界值
		{
			updateNeedGen = false;
			gNullCount.set(0);
			return true;
		}

		if (gNullCount.get() > 0)// 上次generate没有数据
			return false;

		if (genSegCount.get() == 0) //
			return true;
		if (genSegCount.get() > genFirst - 3)// 消耗2个需补充
			return false;
		// 一个或2个// 有fetch or parse
		if (fetchSegCount.get() > 0 || fetching.get() > 0)
			return false;

		return true;
	}

	/**
	 * 不一定非常精确
	 * 
	 * @return
	 */
	private boolean shouldUpdate() {
		if (minCrawlDelay == 0f) {// 抓内网
			parsedSegments.clear();// 消费掉
			parSegCount.set(0);

			return false;
		}

		if (parSegCount.get() == 0)
			return false;
		if (parSegCount.get() >= updateThreshold)// 2个及以上需要更新
			return true;
		// 一个已解析好
		if (genSegCount.get() > 0)// 有segment没fetch
			return false;

		return true;
	}

	protected void generate(int segCount) throws Exception {
		Path[] tmp = generator.generate(crawldb, rootSeg, fetchMap, topN, System.currentTimeMillis(), filter,
				normalise, false, segCount, tableDepth);
		if (tmp == null) {
			LOG.info("generate: 没有生成任何segment");
			gNullCount.incrementAndGet();
			return;
		}
		gNullCount.set(0);
		for (Path path : tmp) {
			generatedSegments.add(path);
			genSegCount.incrementAndGet();
		}
		LOG.info("generate: 生成待抓取的urls：" + Arrays.asList(tmp));

		if (segCount == genFirst)
			genNum = 2;
		if (segCount > tmp.length)// 最后一轮没有generate到数据
			gNullCount.incrementAndGet();
	}

	private void update(final boolean normalize, final boolean filter) throws Exception {
		toUpdate.clear();
		try {
			Path seg = null;
			while ((seg = parsedSegments.poll()) != null) {
				toUpdate.add(seg);
				parSegCount.decrementAndGet();
			}
			if (toUpdate.size() == 0) {
				LOG.info(Thread.currentThread() + " 没有目录需要合并到crawldb。");
				return;
			}
			LOG.info(Thread.currentThread() + " 更新crawldb:从该目录：" + toUpdate);
			crawlDbTool.update(crawldb, toUpdate.toArray(new Path[0]), normalize, filter);
			LOG.info(Thread.currentThread() + " 更新crawldb:完成segment的更新：" + toUpdate);
			updateNeedGen = true;
		} catch (Exception e) {
			e.printStackTrace();
			for (Path path : toUpdate) {
				parsedSegments.add(path);
				parSegCount.incrementAndGet();
			}
		}
	}

	protected void fetch(final int num) throws Exception {
		Thread t = new Thread() {
			public void run() {
				Thread.currentThread().setName("fetch线程" + num);
				LOG.info(Thread.currentThread() + ": 线程启动");
				while (true) {
					if (fetchJobing.compareAndSet(false, true)) {
						Path seg = null;
						try {
							seg = generatedSegments.poll(3, TimeUnit.SECONDS);
						} catch (Exception e1) {
							e1.printStackTrace();
						}
						currentFetch = seg;
						if (seg != null) {
							genSegCount.decrementAndGet();
							fetching.incrementAndGet();

							LOG.info(Thread.currentThread() + ": 开始抓取目录：" + seg);
							try {
								fetcher.fetch(seg, NutchParallelUtilHBase2.threads);// fetch

								fetchedSegments.add(seg);// 成功放入队列供下步处理
								fetchSegCount.incrementAndGet();
							} catch (Exception e) {
								e.printStackTrace();
								generatedSegments.add(seg);// 失败返回原队列
								genSegCount.incrementAndGet();
							}
							fetching.decrementAndGet();
							LOG.info(Thread.currentThread() + ": 结束抓取目录：" + seg);
						}
						if (seg == currentFetch)// 没有其他人在抓
							fetchJobing.compareAndSet(true, false);
					} else {
						Path doing = currentFetch;
						if (doing != null) {
							try {
								while (!FetchNotify.fetchDone(doing.getName(), fetchMap - 3)) {
									try {
										Thread.sleep(3000);
									} catch (InterruptedException e) {
										e.printStackTrace();
									}
								}
								LOG.info(Thread.currentThread() + ":fetchjob id done, path=" + doing);
								if (doing == currentFetch && genSegCount.get() > 0) {// 有东西需要抓
									LOG.info(Thread.currentThread() + ":预先启动下一轮fetch，genSegCount=" + genSegCount.get());
									fetchJobing.set(false);// 好启动新的fetch
								} else {
									LOG.info(Thread.currentThread() + ":未能预先启动下一轮fetch，genSegCount="
											+ genSegCount.get());
									try {
										Thread.sleep(5000);
									} catch (InterruptedException e1) {
										e1.printStackTrace();
									}
								}
							} catch (Exception e) {
								e.printStackTrace();
								try {
									Thread.sleep(10000);
								} catch (InterruptedException e1) {
									e1.printStackTrace();
								}
							}
						} else {
							try {
								Thread.sleep(3000);
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
						}
					}
				}

			}
		};
		t.setDaemon(true);
		t.start();
	}

	protected static void parse() throws Exception {
		for (int i = 0; i < 1; i++) {
			final int num = i;
			Thread t = new Thread() {
				public void run() {
					Thread.currentThread().setName("parseThread" + num);
					LOG.info(Thread.currentThread() + ": 线程启动");
					while (true) {
						Path seg = null;
						try {
							seg = fetchedSegments.poll(5, TimeUnit.SECONDS);
						} catch (InterruptedException e1) {
							e1.printStackTrace();
						}
						if (seg != null) {
							fetchSegCount.decrementAndGet();

							LOG.info(Thread.currentThread() + ":开始处理目录：" + seg);
							try {
								parser.parse(seg);

								parsedSegments.add(seg);// 成功放入队列供下步处理
								parSegCount.incrementAndGet();
							} catch (Exception e) {
								e.printStackTrace();
								fetchedSegments.add(seg);// 失败返回原队列
								fetchSegCount.incrementAndGet();
							}
							LOG.info(Thread.currentThread() + ":结束处理目录：" + seg);
						}
					}

				}
			};
			t.setDaemon(true);
			t.start();
		}
	}

	public static class FetchNotify {
		private final static String tableName = "urlid";
		static HConnection connection = null;
		static HTableInterface table = null;

		// The following properties are localized in the job configuration for
		// each task's execution:
		// Name Type Description
		// mapred.job.id String The job id
		// mapred.jar String job.jar location in job directory
		// job.local.dir String The job specific shared scratch space
		// mapred.tip.id String The task id
		// mapred.task.id String The task attempt id
		// mapred.task.is.map boolean Is this a map task
		// mapred.task.partition int The id of the task within the job
		public static void notifyFetchMapEnd(Configuration conf) {
			fetchAhead = conf.getBoolean("fetcher.nextstart.ahead", false);
			if (!fetchAhead)
				return;

			// String jobId = conf.get("mapred.job.id");
			// String path = conf.get("mapred.output.dir");
			String path = conf.get(Nutch.SEGMENT_NAME_KEY);
			int partition = conf.getInt("mapred.task.partition", 0);

			HConnection connection = null;
			HTableInterface table = null;
			try {
				connection = HConnectionManager.createConnection(HBaseConfiguration.create());
				table = connection.getTable(tableName);

				// get HBase configuration
				LOG.info("save to hbase partition=" + partition + " path=" + path);
				Put put = new Put(Bytes.toBytes(path + "@" + partition));
				// 参数分别：列族、列、值
				put.add(Bytes.toBytes("cf1"), Bytes.toBytes("p"), Bytes.toBytes(partition));
				table.put(put);
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				try {
					table.close();
					connection.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		public static boolean fetchDone(String path, int partNum) throws Exception {
			if (connection == null) {
				connection = HConnectionManager.createConnection(HBaseConfiguration.create());
				table = connection.getTable(tableName);
			}

			Scan scan = new Scan();
			scan.setStartRow(Bytes.toBytes(path + "@"));
			scan.setStopRow(Bytes.toBytes(path + "@@"));
			ResultScanner rs = table.getScanner(scan);
			long cnt = 0;
			for (Result r : rs) {
				cnt++;
			}
			rs.close();
			// table.close();
			// connection.close();

			return partNum == cnt;
		}

	}
}
