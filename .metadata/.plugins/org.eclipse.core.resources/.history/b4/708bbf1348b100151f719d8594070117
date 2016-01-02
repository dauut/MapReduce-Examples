package mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class AvarageDollarsObligated {

	// Mapper class
	public static class E_EMapper extends MapReduceBase
			implements Mapper<LongWritable, /* Input key Type */
			Text, /* Input value Type */
			IntWritable, /* Output key Type */
			DoubleWritable> /* Output value Type */
	{
		private final static IntWritable one = new IntWritable(1); // intwritable
		private final static DoubleWritable two = new DoubleWritable();

		// Map function
		public void map(LongWritable key, Text value, OutputCollector<IntWritable, DoubleWritable> output,
				Reporter reporter) throws IOException {

			//fix empty line 
			String line = value.toString();
			if (line == null) {
				line = "\"0\"";
			}
			String[] split = line.split(",");
			if (split.length < 2) {
				two.set(0.0);
				output.collect(one, two);
			} else {
				String lasttoken = null;

				lasttoken = split[2];
				if (lasttoken.equals("\"dollarsobligated\"")) {
					lasttoken = "\"0\"";
				}
				String dollarsObligatedVal = lasttoken.substring(1, lasttoken.length() - 1);

				if (dollarsObligatedVal != null) {
					try {
						two.set(Double.parseDouble(dollarsObligatedVal));
					} catch (NumberFormatException e) {
						e.printStackTrace();
					}
					output.collect(one, two);
				} else {
					two.set(0.0);
					output.collect(one, two);
				}
			}

		}

	}

	// Reducer class
	public static class E_EReduce extends MapReduceBase
			implements Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
		// private final static IntWritable one = new IntWritable(1);
		private final static DoubleWritable avarage = new DoubleWritable();

		// Reduce function
		public void reduce(IntWritable key, Iterator<DoubleWritable> values,
				OutputCollector<IntWritable, DoubleWritable> output, Reporter reporter) throws IOException {

			double sum = 0;
			int count = 0;
			while (values.hasNext()) {
				sum += values.next().get();
				count++;
			}
			avarage.set(sum / (double) count);
			output.collect(key, avarage);
		}

	}

	public static void main(String args[]) throws Exception {
		JobConf conf = new JobConf(AvarageDollarsObligated.class);

		conf.setJobName("Avarage_dollars_obligated");
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(DoubleWritable.class);
		conf.setMapperClass(E_EMapper.class);
		conf.setCombinerClass(E_EReduce.class);
		conf.setReducerClass(E_EReduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);
	}
};