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

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Generator;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.URLFilterException;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.TimingUtil;
// rLogging imports
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates a subset of a crawl db to fetch. This version allows to generate
 * fetchlists for several segments in one go. Unlike in the initial version
 * (OldGenerator), the IP resolution is done ONLY on the entries which have been
 * selected for fetching. The URLs are partitioned by IP, domain or host within
 * a segment. We can chose separately how to count the URLS i.e. by domain or
 * host to limit the entries.
 **/
public class GeneratorHbase2 extends Generator {
	public static final Logger LOG = LoggerFactory.getLogger(GeneratorHbase2.class);
	static final String GENERATL_CNT = "generate.cnt";
	static final String GENERATL_TABLE = "generate.table";
	static final String GENERATL_REDUCENUM = "generate.reduceNum";

	/** Selects entries due for fetch. */
	public static class GenerateMark implements Mapper<Text, CrawlDatum, Text, CrawlDatum> {
		private boolean filter;
		private URLFilters filters;
		JobConf conf;

		private HConnection connection;
		private HTableInterface table;
		private long cnt = 0;

		public void configure(JobConf job) {
			conf = job;
			filter = job.getBoolean(GENERATOR_FILTER, true);
			if (filter)
				filters = new URLFilters(job);

			String tableName = job.get(GENERATL_TABLE);
			try {
				connection = HConnectionManager.createConnection(job);
				table = connection.getTable(tableName);
				table.setAutoFlush(false, true);
				table.setWriteBufferSize(12 * 1024 * 1024);
			} catch (IOException e) {
				e.printStackTrace();
			}
			cnt = 0;
		}

		public void close() {
			try {
				table.flushCommits();
				table.close();
				connection.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		public void map(Text key, CrawlDatum value, OutputCollector<Text, CrawlDatum> output, Reporter reporter)
				throws IOException {
			if (filter) {
				// If filtering is on don't generate URLs that don't pass
				// URLFilters
				try {
					if (filters.filter(key.toString()) == null)
						return;
				} catch (URLFilterException e) {
					if (LOG.isWarnEnabled()) {
						LOG.warn("Couldn't filter url: " + key + " (" + e.getMessage() + ")");
					}
				}
			}

			Put put = TableReader.generatedPut(key.getBytes(), value);
			table.put(put);
			if (++cnt % 10000 == 0) {
				table.flushCommits();
			}

			output.collect(key, value);
			reporter.incrCounter("Generator", "records", 1);
		}
	}

	public GeneratorHbase2() {
	}

	public GeneratorHbase2(Configuration conf) {
		setConf(conf);
	}

	/**
	 * Generate fetchlists in one or more segments. Whether to filter URLs or
	 * not is read from the crawl.generate.filter property in the configuration
	 * files. If the property is not found, the URLs are filtered. Same for the
	 * normalisation.
	 * 
	 * @param dbDir
	 *            Crawl database directory
	 * @param segments
	 *            Segments directory
	 * @param numLists
	 *            Number of reduce tasks
	 * @param topN
	 *            Number of top URLs to be selected
	 * @param curTime
	 *            Current time in milliseconds
	 * 
	 * @return Path to generated segment or null if no entries were selected
	 * 
	 * @throws IOException
	 *             When an I/O error occurs
	 */
	public Path[] generate(Path dbDir, Path segments, int numLists, long topN, long curTime, boolean filter,
			boolean norm, boolean force, int maxNumSegments, int tableDepth) throws IOException {
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

		if (maxNumSegments == -1)
			maxNumSegments = 1;
		List<Path> generatedSegments = new ArrayList<Path>();
		int j = 0;// use for tablename
		String table = null;
		boolean isSmart = getConf().getBoolean("nutch.smart.is", false);

		for (int i = 0; i < maxNumSegments; i++) {
			Path segment = null;
			long cnt = 0;
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
				table = dbDir.getName() + j;
				RunningJob r = generateJob(table, segment, numLists, topN - cnt, curTime, filter, norm, force);
				Counter counter = r.getCounters().findCounter("Generator", "records");
				cnt += counter.getValue();
				LOG.info("Generator: records: " + cnt + " current table=" + table + " timeused="
						+ (System.currentTimeMillis() - begin));
			}
			generatedSegments.add(segment);
			j--;

		}

		long end = System.currentTimeMillis();
		LOG.info("Generator: finished at " + sdf.format(end) + ", elapsed: " + TimingUtil.elapsedTime(start, end));

		if (generatedSegments.size() > 0)
			return generatedSegments.toArray(new Path[generatedSegments.size()]);
		else
			return null;
	}

	private RunningJob generateJob(String table, Path segment, int numLists, long topN, long curTime, boolean filter,
			boolean norm, boolean force) throws IOException {
		LOG.info("Generator: segment: " + segment);

		JobConf job = new NutchJob(getConf());
		job.setJarByClass(GeneratorHbase2.class);
		job.setJobName("generate: from " + table + " "
				+ (new SimpleDateFormat("yyyyMMdd HH:mm:ss")).format(System.currentTimeMillis()));
		// job.setLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 300000);

		if (numLists == -1) {
			numLists = job.getNumMapTasks(); // a partition per fetch task
		}
		numLists = 4;// TODO
		if ("local".equals(job.get("mapred.job.tracker")) && numLists != 1) {
			// override
			LOG.info("Generator: jobtracker is 'local', generating exactly one partition.");
			numLists = 1;
		}
		// job.setLong(GENERATOR_CUR_TIME, curTime);
		// record real generation time
		long generateTime = System.currentTimeMillis();
		job.setLong(Nutch.GENERATE_TIME_KEY, generateTime);
		job.setLong(GENERATOR_TOP_N, topN);
		job.setBoolean(GENERATOR_FILTER, filter);
		job.setBoolean(GENERATOR_NORMALISE, norm);
		job.set(GENERATL_TABLE, table);
		job.setInt(GENERATL_REDUCENUM, numLists);

		job.setInputFormat(TableTopInputFormat.class);// 查询数据
		job.setMapperClass(GenerateMark.class);// 标记generate?

		job.setPartitionerClass(URLCountPartitioner.class);
		job.setNumReduceTasks(numLists);
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
		int res = ToolRunner.run(NutchConfiguration.create(), new GeneratorHbase2(), args);
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
			Path[] segs = generate(new Path("/data/crawldb"), new Path("/data/segments"), 4, topN, 0, false, false,
					false, 1, 1);
			if (segs == null)
				return -1;
		} catch (Exception e) {
			LOG.error("Generator: " + StringUtils.stringifyException(e));
			return -1;
		}
		return 0;
	}
}
