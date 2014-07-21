package org.hbase;

import java.io.IOException;
import java.sql.Connection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HbaseMR extends Configured implements Tool {
	public static Configuration conf = HBaseConfiguration.create();
	static final Log LOG = LogFactory.getLog(HbaseMR.class);

	/**
	 * 自定义文件写表
	 * 
	 * @author xxhuang
	 */
	public static class Map extends Mapper<LongWritable, Text, NullWritable, NullWritable> {
		Configuration configuration = null;
		HConnection connection = null;
		HTableInterface table = null;
		private boolean wal = true;
		private Put put = null;
		static long count = 0;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			configuration = context.getConfiguration();
			connection = HConnectionManager.createConnection(configuration);
			table = connection.getTable("");
			table.setAutoFlush(false, true);
			table.setWriteBufferSize(12 * 1024 * 1024);
			wal = true;
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String all[] = value.toString().split("/t");
			if (all.length == 2) {
				put = new Put(Bytes.toBytes(all[0]));
				put.add(Bytes.toBytes("xxx"), Bytes.toBytes("20110313"), Bytes.toBytes(all[1]));
			}

			if (!wal) {
				put.setDurability(Durability.SKIP_WAL);
			}

			table.put(put);
			if ((++count % 100) == 0) {
				context.setStatus(count + " DOCUMENTS done!");
				context.progress();
				System.out.println(count + " DOCUMENTS done!");
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			super.cleanup(context);
			table.flushCommits();
			table.close();
			connection.close();
		}

	}

	public int run(String[] args) throws Exception {
		String input = args[0];
		Job job = new Job(conf, "jobName");
		job.setJarByClass(HbaseMR.class);
		job.setMapperClass(Map.class);
		job.setNumReduceTasks(0);
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.setInputPaths(job, input);
		job.setOutputFormatClass(NullOutputFormat.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		int res = 1;
		try {
			res = ToolRunner.run(conf, new HbaseMR(), otherArgs);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(res);

	}

	public static void readHtable() throws Exception {
		Job job = new Job(conf, "ExampleRead");
		job.setJarByClass(HbaseMR.class);
		Scan scan = new Scan();
		scan.setCaching(500); // 1 is the default in Scan, which will be bad for
								// MapReduce jobs
		scan.setCacheBlocks(false); // don't set to true for MR jobs
		TableMapReduceUtil.initTableMapperJob("", // input HBase table name
				scan, // Scan instance to control CF and attribute selection
				MyMapper.class, // mapper
				null, // mapper output key
				null, // mapper output value
				job);
		job.setOutputFormatClass(NullOutputFormat.class); // because we aren't
															// emitting anything
															// from mapper
		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
	}

	public static class MyMapper extends TableMapper<Text, Text> {

		public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException,
				IOException {
			// process data for the row from the Result instance.
		}
	}

	public static void readWriteHtable() throws Exception {
		Job job = new Job(conf, "ExampleReadWrite");
		job.setJarByClass(HbaseMR.class); // class that contains mapper

		Scan scan = new Scan();
		scan.setCaching(500); // 1 is the default in Scan, which will be bad for
								// MapReduce jobs
		scan.setCacheBlocks(false); // don't set to true for MR jobs
		// set other scan attrs

		TableMapReduceUtil.initTableMapperJob("sourceTable", // input table
				scan, // Scan instance to control CF and attribute selection
				MyMapper2.class, // mapper class
				null, // mapper output key
				null, // mapper output value
				job);
		TableMapReduceUtil.initTableReducerJob("targetTable", // output table
				null, // reducer class
				job);
		job.setNumReduceTasks(0);

		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
	}

	public static class MyMapper2 extends TableMapper<ImmutableBytesWritable, Put> {

		public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException,
				InterruptedException {
			// this example is just copying the data from the source table...
			context.write(row, resultToPut(row, value));
		}

		private static Put resultToPut(ImmutableBytesWritable key, Result result) throws IOException {
			Put put = new Put(key.get());
			for (Cell kv : result.rawCells()) {
				put.add(kv);
			}
			return put;
		}
	}

	public static void summaryHtable() throws Exception {
		Job job = new Job(conf, "ExampleSummary");
		job.setJarByClass(HbaseMR.class); // class that contains mapper

		Scan scan = new Scan();
		scan.setCaching(500); // 1 is the default in Scan, which will be bad for
								// MapReduce jobs
		scan.setCacheBlocks(false); // don't set to true for MR jobs
		// set other scan attrs

		TableMapReduceUtil.initTableMapperJob("sourceTable", // input table
				scan, // Scan instance to control CF and attribute selection
				MyMapper3.class, // mapper class
				Text.class, // mapper output key
				IntWritable.class, // mapper output value
				job);
		TableMapReduceUtil.initTableReducerJob("targetTable", // output table
				MyTableReducer3.class, // reducer class
				job);
		job.setNumReduceTasks(1); // at least one, adjust as required

		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
	}

	public static class MyMapper3 extends TableMapper<Text, IntWritable> {
		public static final byte[] CF = "cf".getBytes();
		public static final byte[] ATTR1 = "attr1".getBytes();

		private final IntWritable ONE = new IntWritable(1);
		private Text text = new Text();

		public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException,
				InterruptedException {
			String val = new String(value.getValue(CF, ATTR1));
			text.set(val); // we can only emit Writables...

			context.write(text, ONE);
		}
	}

	public static class MyTableReducer3 extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {
		public static final byte[] CF = "cf".getBytes();
		public static final byte[] COUNT = "count".getBytes();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
				InterruptedException {
			int i = 0;
			for (IntWritable val : values) {
				i += val.get();
			}
			Put put = new Put(Bytes.toBytes(key.toString()));
			put.add(CF, COUNT, Bytes.toBytes(i));

			context.write(null, put);
		}
	}

	public static void htableFile() throws Exception {
		Job job = new Job(conf, "ExampleSummaryToFile");
		job.setJarByClass(HbaseMR.class); // class that contains mapper

		Scan scan = new Scan();
		scan.setCaching(500); // 1 is the default in Scan, which will be bad for
								// MapReduce jobs
		scan.setCacheBlocks(false); // don't set to true for MR jobs
		// set other scan attrs

		TableMapReduceUtil.initTableMapperJob("sourceTable", // input table
				scan, // Scan instance to control CF and attribute selection
				MyMapper.class, // mapper class
				Text.class, // mapper output key
				IntWritable.class, // mapper output value
				job);
		job.setReducerClass(MyReducer4.class); // reducer class
		job.setNumReduceTasks(1); // at least one, adjust as required
		FileOutputFormat.setOutputPath(new JobConf(conf), new Path("/tmp/mr/mySummaryFile")); // adjust
		// directories
		// as
		// required

		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
	}

	public static class MyReducer4 extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
				InterruptedException {
			int i = 0;
			for (IntWritable val : values) {
				i += val.get();
			}
			context.write(key, new IntWritable(i));
		}
	}

	public static class MyRdbmsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		private Connection c = null;

		public void setup(Context context) {
			// create DB connection...
		}

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
				InterruptedException {
			// do summarization
			// in this example the keys are Text, but this is just an example
		}

		public void cleanup(Context context) {
			// close db connection
		}

	}

	public class MyMapper5 extends TableMapper<Text, LongWritable> {
		private HTable myOtherTable;

		public void setup(Context context) {
			// myOtherTable = new HTable(conf, "myOtherTable");
		}

		public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException,
				InterruptedException {
			// process Result...
			// use 'myOtherTable' for lookups
		}
	}
}