/**
 * 
 */
package org.test.hadoop;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
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
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.CrawlDb;
import org.apache.nutch.fetcher.Fetcher;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.ParseSegment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * @author xxhuang
 * 
 */
public class NutchParallelUtilHBase {
	// Runnable不会返回结果，并且无法抛出经过检查的异常而Callable又返回结果，而且当获取返回结果时可能会抛出异常。
	// Callable中的call()方法类似Runnable的run()方法，区别同样是有返回值，后者没有。
	// 如果Future的返回尚未完成，则get（）方法会阻塞等待，直到Future完成返回，可以通过调用isDone（）方法判断Future是否完成了返回。
	// lock.lockInterruptibly();//获取响应中断锁
	// reader.interrupt(); //尝试中断读线程
	// while(!fs.isDone);//Future返回如果没有完成，则一直循环等待，直到Future返回完成
	public static Logger LOG = LoggerFactory.getLogger(NutchParallelUtilHBase.class);

	protected static int genMax = 20;
	protected static int genCount = 2;
	protected static int genFirst = 4;
	protected static int updateThreshold = 2;
	protected static int fetchMap = 8;
	protected static int tableDepth = 10;
	protected static float minCrawlDelay = 0.0f;
	protected static boolean fetchAhead = false;// 提前启动fetchjob开关

	protected static JobConf job;
	protected static GeneratorHbase generator = null;
	protected static Fetcher fetcher = null;
	protected static ParseSegment parser = null;
	protected static CrawlDb crawlDbTool = null;
	protected static Path rootSeg = null;
	protected static Path crawldb = null;
	protected static boolean filter = true;
	protected static boolean normalise = true;
	protected static long topN = 1000;
	protected static int threads = 5;
	// add by tong
	public static final String oldSuffixName = "_f";
	public static final String newSuffixName = "_p";

	// 容易死锁啊
	protected BlockingQueue<Path> generatedSegments = new ArrayBlockingQueue<Path>(genMax, true);
	// 记录Content目录下各子目录路径 add // by // tongw
	protected BlockingQueue<Path> toParseParentSegs = new ArrayBlockingQueue<Path>(genMax * 10, true);
	protected BlockingQueue<Path> toParseSonSegs = new ArrayBlockingQueue<Path>(genMax * 50, true);
	// for update
	protected BlockingQueue<Path> parsedSegments = new ArrayBlockingQueue<Path>(genMax * 50, true);

	protected AtomicInteger genNullCount = new AtomicInteger(0);
	protected AtomicInteger genedSegCount = new AtomicInteger(0);
	protected AtomicInteger fetching = new AtomicInteger(0);// 正在抓取标识
	protected AtomicInteger toParseParentSegCount = new AtomicInteger(0);
	protected AtomicInteger parsedSegCount = new AtomicInteger(0);

	protected List<Path> toUpdate = new ArrayList<Path>();// 批量更新
	protected boolean updateNeedGen = false;// 更新后，值改变
	// fetch job // 令牌
	protected AtomicInteger fetchToken = new AtomicInteger(0);
	protected Path lastFetch = null;// fetch争用用
	protected SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
	// 删除generate文件用
	protected ExecutorService executorService = Executors.newCachedThreadPool(new ThreadFactoryBuilder()
			.setNameFormat("crawl_gen_delete-%d").setDaemon(true).build());

	public static void init(JobConf job, GeneratorHbase generator, Fetcher fetcher, ParseSegment parseSegment,
			CrawlDb crawlDbTool, int fetchThreads, int genFirst, int updateHold) {
		init(job, generator, fetcher, parseSegment, crawlDbTool, fetchThreads, genFirst, updateHold,
				job.getInt("job.reduce.num", 8));
	}

	public static void init(JobConf job, GeneratorHbase generator, Fetcher fetcher, ParseSegment parseSegment,
			CrawlDb crawlDbTool, int fetchThreads, int generate, int uThreshold, int fetchMap) {
		job.setBoolean("fetcher.parse", false);
		filter = job.getBoolean(GeneratorHbase.GENERATOR_FILTER, true);
		normalise = job.getBoolean(GeneratorHbase.GENERATOR_NORMALISE, true);
		threads = fetchThreads;

		genFirst = generate;
		if (genFirst > genMax)
			genFirst = genMax;
		if (genFirst <= genCount)
			genFirst = genCount + 1;
		genCount = genFirst;
		updateThreshold = uThreshold;
		tableDepth = job.getInt("generate.table.depth", 10);
		minCrawlDelay = job.getFloat("fetcher.server.min.delay", 0.0f);
		fetchAhead = job.getBoolean("fetcher.nextstart.ahead", false);
		NutchParallelUtilHBase.fetchMap = fetchMap;
		NutchParallelUtilHBase.job = job;
		NutchParallelUtilHBase.generator = generator;
		NutchParallelUtilHBase.fetcher = fetcher;
		NutchParallelUtilHBase.parser = parseSegment;
		NutchParallelUtilHBase.crawlDbTool = crawlDbTool;
		NutchParallelUtilHBase.fetcher.getConf().setInt("fetchMap", fetchMap);

	}

	public void process(Path crawlDb, Path segments, int reduceCnt, long topN, boolean force) throws Exception {
		NutchParallelUtilHBase.topN = topN;
		NutchParallelUtilHBase.rootSeg = segments;
		NutchParallelUtilHBase.crawldb = crawlDb;

		// generate new segment
		generateUpdate();
		fetch(1); // fetch it
		if (fetchAhead)
			fetch(2);
		parse();
		parseSon();

		check();
	}

	protected void check() throws Exception {
		long cnt = 0;
		while (true) {
			if (genNullCount.get() > 0 && genCount == genFirst)
				throw new Exception("第一次没有generate到数据，程序退出.................................");
			if (genNullCount.get() > 0) {
				if (cnt++ == 360)
					throw new Exception("over 60 minutes 都没有url被generate，程序退出.................................");
			} else {
				cnt = 0;
			}

			threadSleep(10000);
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
						genAll();
						LOG.info("generator: 完成generate。");
					}
					if (shouldUpdate()) {
						LOG.info("updater: 开始update crawldb。");
						try {
							update(true, true);
						} catch (Exception e) {
							LOG.error(Thread.currentThread().toString(), e);
						}
						LOG.info("updater: 完成update crawldb。");
					}
					threadSleep(5000);
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
		if (updateNeedGen && genedSegCount.get() < genCount)// 更新了url库,并且已经generate的数目小于临界值
		{
			updateNeedGen = false;
			genNullCount.set(0);
			return true;
		}
		if (genNullCount.get() > 0)// 上次generate没有数据
			return false;

		if (genedSegCount.get() == 0) //
			return true;
		if (genedSegCount.get() > genFirst - 2)// 消耗2个需补充
			return false;
		// 一个或2个// 有fetch
		if (fetching.get() > 0)
			return false;

		return true;
	}

	/**
	 * 不一定非常精确
	 * 
	 * @return
	 */
	private boolean shouldUpdate() {
		if (minCrawlDelay == 0f) {// 测试抓内网，不更新
			parsedSegments.clear();// 消费掉
			parsedSegCount.set(0);
			return false;
		}

		if (parsedSegCount.get() == 0)
			return false;
		if (parsedSegCount.get() >= updateThreshold)// 2个及以上需要更新
			return true;
		// 有解析好的
		if (genedSegCount.get() > 0)// 有segment没fetch
			return false;

		return true;
	}

	protected void genAll() {
		int tableMax = job.getInt("generate.table.Max", 10);
		int success = 0;
		for (int i = tableMax; i > tableMax - tableDepth; i--) {
			Path tmp = generator.generateAll(i, rootSeg, fetchMap, filter, normalise, false);
			if (tmp != null) {
				success++;
				generatedSegments.add(tmp);
				genedSegCount.incrementAndGet();
				LOG.info("generate: 生成待抓取的urls：" + tmp);
			}
			if (shouldUpdate())
				return;
		}
		if (success == 0) {
			LOG.info("generate: 这轮没有生成任何segments..............................");
			genNullCount.incrementAndGet();
		} else {
			genNullCount.set(0);
			genCount = 2;// 第一轮有记录
		}
	}

	protected void generate(int segCount) throws Exception {
		Path[] tmp = generator.generate(rootSeg, fetchMap, topN, filter, normalise, false, segCount, tableDepth);
		if (tmp == null) {
			LOG.info("generate: 没有生成任何segment");
			genNullCount.incrementAndGet();
			return;
		}
		genNullCount.set(0);
		for (Path path : tmp) {
			generatedSegments.add(path);
			genedSegCount.incrementAndGet();
		}
		LOG.info("generate: 生成待抓取的urls：" + Arrays.asList(tmp));

		genCount = 2;
		if (segCount > tmp.length)// 最后一轮没有generate到数据
			genNullCount.incrementAndGet();
	}

	private void update(final boolean normalize, final boolean filter) throws Exception {
		toUpdate.clear();
		try {
			Path seg = null;
			while ((seg = parsedSegments.poll()) != null) {
				toUpdate.add(seg);
				parsedSegCount.decrementAndGet();
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
			LOG.error(toUpdate.toString(), e);
			for (Path path : toUpdate) {
				parsedSegments.add(path);
				parsedSegCount.incrementAndGet();
			}
		}
	}

	protected void fetch(final int num) throws Exception {
		Thread t = new Thread() {
			public void run() {
				Thread.currentThread().setName("fetch线程" + num);
				LOG.info(Thread.currentThread() + ": 线程启动");

				fetchToken.compareAndSet(0, num);// 抢令牌
				while (true) {
					if (fetchToken.compareAndSet(num, num)) {
						lastFetch = null;
						fetching.incrementAndGet();
						Path seg = null;
						try {
							seg = generatedSegments.poll(60, TimeUnit.SECONDS);
						} catch (Exception e1) {
						}
						lastFetch = seg;
						if (seg != null) {
							genedSegCount.decrementAndGet();
							LOG.info(Thread.currentThread() + ": 开始fetch目录：" + seg);
							try {
								// modify by// tongw
								toParseParentSegs.add(seg);// 抓取行为与之前有所变化，每次抓取前放入segment信息标识为正在抓取的目录
								toParseParentSegCount.incrementAndGet();

								fetcher.fetch(seg, NutchParallelUtilHBase.threads);// fetch
								// fetchedSegments.add(seg);

								final Path curGen = new Path(seg, CrawlDatum.GENERATE_DIR_NAME);
								executorService.submit(new Runnable() {
									@Override
									public void run() {
										try {
											FileSystem fs = FileSystem.get(job);
											fs.delete(curGen, true);
											LOG.info(Thread.currentThread().toString() + curGen
													+ "目录抓取结束，删除crawl_generate目录。");
										} catch (Exception e) {
											LOG.error(Thread.currentThread() + e.toString());
										}
									}
								});
							} catch (Exception e) {
								LOG.error("fetch: " + seg, e);
								generatedSegments.add(seg);// 失败返回原队列
								genedSegCount.incrementAndGet();
							}
							LOG.info(Thread.currentThread() + ": 结束fetch目录：" + seg);
						}

						if (fetchToken.compareAndSet(num, num))// 没有其他fetch
							lastFetch = null;
						fetching.decrementAndGet();
					} else {
						Path doing = lastFetch;
						if (doing != null) {
							try {
								while (!FetchNotify.fetchDone(doing.getName(), fetchMap - 4)) {
									threadSleep(3000);
								}
								LOG.info(Thread.currentThread() + ":fetchjob id done, path=" + doing);
								// 有东西需要抓
								if (doing == lastFetch) {// 好启动新的fetch
									fetchToken.set(num);
									LOG.info(Thread.currentThread() + ":预先启动下一轮fetch，genSegCount="
											+ genedSegCount.get());
								} else {
									LOG.info(Thread.currentThread() + ":未能预先启动下一轮fetch，genSegCount="
											+ genedSegCount.get());
									threadSleep(5000);
								}
							} catch (Exception e) {
								LOG.error(Thread.currentThread() + "等待中的fetch线程有异常：", e);
								threadSleep(10000);
							}
						} else {
							threadSleep(3000);
						}
					}
				}

			}
		};
		t.setDaemon(true);
		t.start();
	}

	protected void parse() throws Exception {
		for (int i = 1; i <= 3; i++) {
			final int num = i;
			Thread t = new Thread() {
				public void run() {
					Thread.currentThread().setName("parseParentThread" + num);
					LOG.info(Thread.currentThread() + ": 线程启动");

					FileSystem fs = null;
					try {
						fs = FileSystem.get(job);
					} catch (IOException e1) {
						LOG.error(Thread.currentThread().toString(), e1);
					}

					while (true) { // fetchedSegments
						Path curFetching = null;// 正在抓取的目录
						while (curFetching == null) {
							threadSleep(1000);
							try {
								curFetching = toParseParentSegs.poll(10, TimeUnit.MINUTES);
							} catch (Exception e) {
							}
						} // 成功获取

						LOG.info(Thread.currentThread() + "开始监测fetch的输出目录：" + curFetching);
						toParseParentSegCount.decrementAndGet();
						boolean end = false;// 这轮抓取是否结束标志
						Path contentdir = new Path(curFetching, Fetcher.CONTENT_REDIR);
						Map pathFetchDone = new HashMap();
						while (!end) {
							try {
								if (fs.exists(contentdir)) {
									FileStatus[] fstats = fs.listStatus(contentdir);
									if (fstats.length > 0) {
										Path[] paths = FileUtil.stat2Paths(fstats);// 有序？
										paths = removeFetchDone(paths, pathFetchDone);
										for (Path p : paths) {
											if (fs.isDirectory(p) && isFetchPathTimeDone(fs, p, paths.length)) {// 有抓取输出
												pathFetchDone.put(p.toString(), null);
												toParseSonSegs.add(p);
												if (p.getName().startsWith("last")) {// 有序就没有问题
													end = true;
													LOG.info(Thread.currentThread() + "监测到last子目录：" + curFetching);
												}
											} else
												break;// 保证last is end
										}
									}
								}
							} catch (Exception e) {
								LOG.error(Thread.currentThread() + "目录监测线程发现异常: " + curFetching, e);
							}
							if (toParseParentSegCount.get() > 20) {
								LOG.error(Thread.currentThread() + " 等待parse的父目录数=" + toParseParentSegCount.get()
										+ " 跳过当前处理的目录=" + curFetching);
								break;// 跳过该目录？
							}
							threadSleep(10 * 1000);
						}
					}
				}
			};
			t.setDaemon(true);
			t.start();
		}
	}

	protected Path[] removeFetchDone(Path[] listPaths, Map pathFetchDone) {
		if (listPaths.length == 0)
			return listPaths;
		if (pathFetchDone.isEmpty()) {
			Arrays.sort(listPaths);
			return listPaths;
		}

		List<Path> tmp = new ArrayList<Path>();
		for (Path path : listPaths) {
			if (pathFetchDone.containsKey(path.toString()))
				continue;
			tmp.add(path);
		}

		Collections.sort(tmp);
		return tmp.toArray(new Path[] {});
	}

	protected boolean isFetchPathTimeDone(FileSystem fs, Path path, int pathCount) throws Exception {
		String timeStr = path.getName();
		if (StringUtils.isNumeric(timeStr)) {
			boolean hasOther = false;
			FileStatus[] fstats = fs.listStatus(path);
			Path[] paths = FileUtil.stat2Paths(fstats);
			for (Path p : paths) {
				if (fs.isDirectory(p) && !p.getName().endsWith(oldSuffixName)) {
					hasOther = true;
					break;
				}
			}

			java.util.Date fetchTime = sdf.parse(timeStr);
			long currentTime = System.currentTimeMillis();
			if (currentTime - fetchTime.getTime() >= 5 * 60 * 1000) { // time
				if (!hasOther)
					return true;
				else {// del
					fs.delete(path, true);
					String pathStr = path.toString();
					pathStr = pathStr.replaceAll(Fetcher.CONTENT_REDIR, CrawlDatum.FETCH_DIR_NAME);
					fs.delete(new Path(pathStr), true);
					LOG.warn(Thread.currentThread() + "超过5分钟都没有写完，删除目录=" + path);
					return false;
				}
			}
			return false;
		} else
			return isPathFetchDone(fs, path, pathCount);
	}

	protected boolean isPathFetchDone(FileSystem fs, Path path, int pathCount) {
		boolean hasFetchDone = false;
		boolean hasOther = false;
		int cnt = 0;
		try {
			FileStatus[] fstats = fs.listStatus(path);
			Path[] paths = FileUtil.stat2Paths(fstats);
			for (Path p : paths) {
				if (fs.isDirectory(p) && p.getName().endsWith(oldSuffixName)) {
					hasFetchDone = true;
					cnt++;
				}
				if (fs.isDirectory(p) && !p.getName().endsWith(oldSuffixName)) {
					hasOther = true;
				}
			}

			if (path.getName().startsWith("last") && !hasOther) {
				return cnt >= fetchMap;
			}
			if (pathCount > 10)// 下个目录都有了，可能还有错，反正任务会重跑
				return hasFetchDone && !hasOther;
			if (cnt >= fetchMap)// 有fetchMap个才确认
				return true;
		} catch (Exception e) {
			LOG.error("isPathFetchDone: " + path, e);
		}

		return false;
	}

	protected void parseSon() {
		for (int i = 1; i <= 10; i++) {// 外部配置化？
			final int num = i;
			Thread t = new Thread() {
				public void run() {
					Thread.currentThread().setName("parseSonThread" + num);
					LOG.info(Thread.currentThread() + ": 线程启动");
					while (true) {
						Path seg = null;
						try {// fetchedSegments
							seg = toParseSonSegs.poll(60, TimeUnit.SECONDS);
						} catch (InterruptedException e1) {
						}
						if (seg != null) {
							LOG.info(Thread.currentThread() + ":开始parse目录：" + seg);
							try {
								parser.parse(seg);

								parsedSegments.add(seg);// 成功放入队列供下步处理
								parsedSegCount.incrementAndGet();
							} catch (Exception e) {
								LOG.error("path=" + seg, e);
								toParseSonSegs.add(seg);// 失败返回原队列
							}
							LOG.info(Thread.currentThread() + ":结束parse目录：" + seg);
						}
					}

				}
			};
			t.setDaemon(true);
			t.start();
		}
	}

	protected void threadSleep(long time) {
		try {
			Thread.sleep(time);
		} catch (Exception e) {
			e.printStackTrace();
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
				LOG.error(path + "@" + partition, e);
			} finally {
				try {
					table.close();
					connection.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		public static boolean fetchDone(String path, int needCnt) throws Exception {
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

			return cnt >= needCnt;
		}

	}
}
