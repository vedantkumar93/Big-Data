import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

@SuppressWarnings("deprecation")
public class MapReduce1 {
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// get all args
		if (otherArgs.length != 2) {
			System.err.println("Usage: MapReduce1 /Vedant/input/soc-LiveJournal1Adj.txt /Vedant/input/output1");
			System.exit(2);
		}

		Job job = new Job(conf, "MapReduce1");
		job.setJarByClass(MapReduce1.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		// uncomment the following line to add the Combiner
		// job.setCombinerClass(Reduce.class);

		job.setMapOutputKeyClass(Text.class); // Setting Map Outputkey
		job.setMapOutputValueClass(Text.class); // Setting Output Value Class for map

		// set output key type
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(IntWritable.class);
		// set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		// Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		private Text keyOut = new Text(); // type of output key
		private Text valueOut = new Text(); // type of output value

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] line = value.toString().split("\t");
			if (line.length == 2) {
				valueOut.set(line[1]);
				String str;
				for (String data : line[1].split(",")) {
					if (Integer.parseInt(data) > Integer.parseInt(line[0]))
						str = line[0].concat(",").concat(data);
					else
						str = data.concat(",").concat(line[0]);
					keyOut.set(str);
					context.write(keyOut, valueOut);
				}
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		private Text keyOut = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			keyOut.set(key);
			String valueOut="";
			List<String> list = new ArrayList<>(Arrays.asList(values.iterator().next().toString().split(",")));
			for (Text value : values)
				list.retainAll(new ArrayList<>(Arrays.asList(value.toString().split(","))));
			if (!list.isEmpty()) {
				for(String str:list)
					valueOut+=str+",";
				valueOut = valueOut.substring(0, valueOut.length()-1);
				context.write(keyOut, new Text(valueOut));
			}
		}
	}
}
