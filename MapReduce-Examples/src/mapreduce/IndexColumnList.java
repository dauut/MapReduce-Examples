package mapreduce;

/*this class is index every depertmant alphabetically with their counts
 * takes their value, if value is empty then it write 3 different word according the case
 * */
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
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

public class IndexColumnList {

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
			String line = value.toString();
			if (line == null) {
				line = "\"empty\"";
			}
			
			String[] split = line.split(",");
			
			if (split.length < 6) {
				word.set("\"emptyval\"");
				output.collect(word, one);
			} else {
				String lasttoken = null;

				lasttoken = split[5];

				String dollarsObligatedVal = lasttoken.substring(1, lasttoken.length() - 1);

				if (dollarsObligatedVal != null) {
					try {
						word.set(lasttoken);
					} catch (NumberFormatException e) {
						e.printStackTrace();
					}
					output.collect(word, one);
				} else {
					word.set("\"emptyvalue\"");
					output.collect(word, one);
				}
			}

		}
	}

	// Reducer class
	public static class E_EReduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

		// Reduce function
		@SuppressWarnings({ "unchecked", "rawtypes" })
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,
				Reporter reporter) throws IOException {
			HashMap<String, Integer> cache = new HashMap<String, Integer>();
			//HashMap<String, Integer> cache_sorted = new HashMap<String, Integer>();
			
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			cache.put(key.toString(), sum);
			
			Object[] a = cache.entrySet().toArray();
			
		    Arrays.sort(a, new Comparator() {
		        public int compare(Object o1, Object o2) {
		            return ((Map.Entry<String, Integer>) o1).getValue().compareTo(
		                    ((Map.Entry<String, Integer>) o2).getValue());
		        }
		    });
		    
		    for (Object e : a) {
		    	output.collect(new Text(((Map.Entry<String, Integer>) e).getKey()), new IntWritable(((Map.Entry<String, Integer>) e).getValue()));
		    }

			
		}
	}

	// Main function
	public static void main(String args[]) throws Exception {

		JobConf conf = new JobConf(IndexColumnList.class);

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