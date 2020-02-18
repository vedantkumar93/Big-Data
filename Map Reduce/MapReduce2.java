import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

@SuppressWarnings("deprecation")
public class MapReduce2 {
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err
					.println("Usage: MapReduce2 /Vedant/input/soc-LiveJournal1Adj.txt /Vedant/input/output2");
			System.exit(2);
		}
		Job job1 = new Job(conf, "MapReduce2, Phase1");
		job1.setJarByClass(MapReduce2.class);
		job1.setMapperClass(Map1.class);
		job1.setReducerClass(Reduce1.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1].concat("_1")));
		boolean waitForCompletion = job1.waitForCompletion(true);
		if (waitForCompletion) {
			Job job2 = new Job(conf, "MapReduce2, Phase2");
			job2.setJarByClass(MapReduce2.class);
			job2.setMapperClass(Map2.class);
			job2.setReducerClass(Reduce2.class);
			job2.setInputFormatClass(TextInputFormat.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job2, new Path(otherArgs[1].concat("_1")));
			FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]));
			System.exit(job2.waitForCompletion(true) ? 0 : 1);
		}
	}

	public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {

		private Text keyOut = new Text();
		private Text valueOut = new Text();

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

	public static class Reduce1 extends Reducer<Text, Text, Text, Text> {

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
				//context.write(keyOut, new Text(String.valueOf(list.size()).concat("\t").concat(valueOut)));
				context.write(keyOut, new Text(String.valueOf(list.size()).concat("\t").concat(valueOut)));
			}
		}
	}

	public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {
		private final static Text one = new Text(new String("1"));

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			{
				context.write(one, value);
			}
		}
	}

	public static class Reduce2 extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			HashMap<String, Integer> map = new HashMap<String, Integer>();
			HashMap<String, String> friendsMap = new HashMap<String, String>();
			int count = 1;
			for (Text line : values) {
				String[] fields = line.toString().split("\t");
				if (fields.length == 3) {
					map.put(fields[0], Integer.parseInt(fields[1]));
					friendsMap.put(fields[0], fields[1].concat("\t").concat(fields[2]));
				}
			}
			compareValues updatedValues = new compareValues(map);
			TreeMap<String, Integer> sortedMap = new TreeMap<String, Integer>(updatedValues);
			sortedMap.putAll(map);

			for (Entry<String, Integer> entry : sortedMap.entrySet()) {
				if (count <= 10) {
					context.write(new Text(entry.getKey()), new Text(friendsMap.get(entry.getKey())));

				} else
					break;
				count++;
			}
		}
	}

	// Defining compareValues explicitly so that it can be sorted in Descending
	// Order on Map
	public static class compareValues implements Comparator<String> {
		HashMap<String, Integer> value;

		public compareValues(HashMap<String, Integer> value) {
			this.value = value;
		}

		public int compare(String string1, String string2) {
			if (value.get(string1) >= value.get(string2)) {
				return -1;
			} else {
				return 1;
			}
		}
	}
}
