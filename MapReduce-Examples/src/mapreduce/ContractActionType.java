package mapreduce;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
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

public class ContractActionType {

	// Mapper class
	public static class E_EMapper extends MapReduceBase
			implements Mapper<LongWritable, /* Input key Type */
			Text, /* Input value Type */
			Text, /* Output key Type */
			IntWritable> /* Output value Type */
	{
		private final static IntWritable one = new IntWritable(1); // intwritable
		private Text word = new Text();

		// Map function
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			// String line = value.toString();
			String line = value.toString();
			StringTokenizer s = new StringTokenizer(line, ",");

			while (s.hasMoreElements()) {
				word.set(s.nextToken());
				switch (word.toString()) {
				case "\"B: PURCHASE ORDER\"":
					output.collect(word, one);
					break;
				case "\"A: BPA CALL\"":
					output.collect(word, one);
					break;
				case "\"C: DELIVERY ORDER\"":
					output.collect(word, one);
					break;
				case "\"D: DEFINITIVE CONTRACT\"":
					output.collect(word, one);
					break;
				default:
					break;
				}
			}

		}
	}

	// Reducer class
	public static class E_EReduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

		// Reduce function
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,
				Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));

		}
	}

	// Main function
	public static void main(String args[]) throws Exception {

		JobConf conf = new JobConf(ContractActionType.class);

		conf.setJobName("max_eletricityunits");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setMapperClass(E_EMapper.class);
		conf.setCombinerClass(E_EReduce.class);
		conf.setReducerClass(E_EReduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);
	}
}