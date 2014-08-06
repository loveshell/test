package org.test.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class CodeInputFormat implements InputFormat<IntWritable, IntWritable> {
	public static class CodeReader implements RecordReader<IntWritable, IntWritable> {
		private int current = 0;
		private int reduceNum = 1;

		public CodeReader(int reduceNum) {
			super();
			this.reduceNum = reduceNum;
		}

		public boolean next(IntWritable key, IntWritable value) throws IOException {
			if (current == reduceNum)
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
			return current / reduceNum;
		}
	}

	public static class CustomInputSplit extends org.apache.hadoop.mapreduce.InputSplit implements InputSplit {
		private int reduceNum = 1;

		public CustomInputSplit(int reduceNum) {
			super();
			this.reduceNum = reduceNum;
		}

		public void write(DataOutput out) throws IOException {
		}

		public void readFields(DataInput in) throws IOException {
		}

		public long getLength() throws IOException {
			return reduceNum;// reduce num?
		}

		public String[] getLocations() throws IOException {
			return new String[] {};
		}
	}

	public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
		return new CustomInputSplit[] { new CustomInputSplit(job.getInt(GeneratorHbase2.GENERATL_REDUCENUM, 1)) };
	}

	public RecordReader<IntWritable, IntWritable> getRecordReader(InputSplit split, JobConf job, Reporter reporter)
			throws IOException {
		return new CodeReader(job.getInt(GeneratorHbase2.GENERATL_REDUCENUM, 1));
	}
}