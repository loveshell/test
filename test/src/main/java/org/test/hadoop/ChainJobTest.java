package org.test.hadoop;

import java.io.IOException;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class ChainJobTest {

	public static void main(String[] args) throws IOException {
		JobConf conf = new JobConf(ChainJobTest.class);
		conf.setJobName("chain");
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		JobConf mapper1Conf = new JobConf(false);
		JobConf mapper2Conf = new JobConf(false);
		JobConf reduce1Conf = new JobConf(false);
		JobConf mapper3Conf = new JobConf(false);

		// ChainMapper.addMapper(conf, Mapper1.class, LongWritable.class,
		// Text.class, Text.class, Text.class, true,
		// mapper1Conf);
		// ChainMapper.addMapper(conf, Mapper2.class, Text.class, Text.class,
		// LongWritable.class, Text.class, false,
		// mapper2Conf);
		// ChainReducer.setReducer(conf, Reducer.class, LongWritable.class,
		// Text.class, Text.class, Text.class, true,
		// reduce1Conf);
		// ChainReducer
		// .addMapper(conf, Mapper3.class, Text.class, Text.class,
		// LongWritable.class, Text.class, false, null);
		JobClient.runJob(conf);
	}
}
