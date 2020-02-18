import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
public class MapReduce3 {

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: MapReduce3 /Vedant/input/soc-LiveJournal1Adj.txt /Vedant/input/userdata.txt /Vedant/input/output3");
			System.exit(2);
		}
		conf.set("UserDetails", otherArgs[1]);
		Job job = new Job(conf, "MapReduce3");
		job.setJarByClass(MapReduce3.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		private Text keyOut = new Text();
		private Text valueOut = new Text();
		private HashMap<String, String> userDetails;

		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
			userDetails = new HashMap<String, String>();
			FileSystem file = FileSystem.get(config);
			BufferedReader buffread = new BufferedReader(
					new InputStreamReader(file.open(new Path(config.get("UserDetails")))));

			String userinfo = buffread.readLine();
			while (userinfo != null) {
				String[] s = userinfo.split(",");
				String friendinfo = s[1] + ":" + s[9];
				userDetails.put(s[0].trim(), friendinfo);
				userinfo = buffread.readLine();
			}
			buffread.close();
		}

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] line = value.toString().split("\t");
			if (line.length == 2) {
				String sb = "";
				for(String id: line[1].split(",")) 
					sb = sb.concat(id).concat(":").concat(userDetails.get(id)).concat(",");
				valueOut.set(sb.substring(0, sb.length()-1));
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

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			keyOut.set(key);
			StringBuilder valueOut = new StringBuilder("[");
			List<String> list = new ArrayList<>(Arrays.asList(values.iterator().next().toString().split(",")));
			for (Text value : values)
				list.retainAll(new ArrayList<>(Arrays.asList(value.toString().split(","))));
			if (!list.isEmpty()) {
				for (String str : list) 
					valueOut.append(str.split(":")[1]).append(":").append(str.split(":")[2]).append(",");
				valueOut.setCharAt(valueOut.length()-1, ']');
				context.write(keyOut, new Text(valueOut.toString()));
			}
		}
	}
}
