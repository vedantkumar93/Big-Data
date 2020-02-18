import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

@SuppressWarnings("deprecation")
public class MapReduce4 extends Configured implements Tool{
	
	@Override
	public int run(String[] args) throws Exception {
		
		if (args.length != 3) {
			System.err.println(
					"Usage: MapReduce4 /Vedant/input/soc-LiveJournal1Adj.txt /Vedant/input/userdata.txt /Vedant/input/output4");
			System.exit(2);
		}
		// ------------------- PHASE 1 --------------
		Job job1 = new Job(getConf(), "MapReduce4, Phase1");
		job1.setJarByClass(MapReduce4.class);
		MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, UserDetailsMapper.class);
		MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, FriendsMapper.class);
		job1.setReducerClass(Reduce1.class);
		job1.setPartitionerClass(SecondarySortPartitioner.class);
		job1.setSortComparatorClass(SecondarySortCompositeKeySortComparator.class);
		job1.setGroupingComparatorClass(SecondarySortGroupingComparator.class);
		job1.setMapOutputKeyClass(CompositeKeyWritable.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job1, new Path(args[2].concat("_1")));
		job1.waitForCompletion(true);
		
		// ------------------- PHASE 2 --------------
		Job job2 = new Job(getConf(), "MapReduce4, Phase2");
		job2.setJarByClass(MapReduce4.class);
		MultipleInputs.addInputPath(job2, new Path(args[1]), TextInputFormat.class, UserDetailsMapper.class);
		MultipleInputs.addInputPath(job2, new Path(args[2].concat("_1")), TextInputFormat.class,
				InverseMapper.class);
		job2.setPartitionerClass(SecondarySortPartitioner.class);
		job2.setSortComparatorClass(SecondarySortCompositeKeySortComparator.class);
		job2.setGroupingComparatorClass(SecondarySortGroupingComparator.class);
		job2.setReducerClass(Reduce2.class);
		job2.setMapOutputKeyClass(CompositeKeyWritable.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job2, new Path(args[2].concat("_2")));
		job2.waitForCompletion(true);

		// ------------------- PHASE 3 --------------
		Job job3 = new Job(getConf(), "MapReduce4, Phase3");
		job3.setJarByClass(MapReduce4.class);
		job3.setMapperClass(EmitMapper.class);
		job3.setReducerClass(MaxAgeReducer.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job3, new Path(args[2].concat("_2")));
		FileOutputFormat.setOutputPath(job3, new Path(args[2].concat("_3")));
		job3.waitForCompletion(true);
		
		// ------------------- PHASE 4 --------------
		Job job4 = new Job(getConf(), "MapReduce4, Phase4");
		job4.setJarByClass(MapReduce4.class);
		job4.setMapperClass(AggregateMapper.class);
		job4.setReducerClass(Top10Reducer.class);
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job4, new Path(args[2].concat("_3")));
		FileOutputFormat.setOutputPath(job4, new Path(args[2]));
		boolean success = job4.waitForCompletion(true);
		
		return success ? 0:1;
	
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new MapReduce4(), args);
		System.exit(exitCode);
	}

	// Enables userdata.txt to reach first, then soc-LiveJournal1Adj.txt
	public static class CompositeKeyWritable implements Writable, WritableComparable<CompositeKeyWritable> {

		private Integer userId;
		private Integer operationId; // 0 for UserDetailsMapper, 1 for FriendsMapper and InverseMapper

		public CompositeKeyWritable(Integer userId, Integer operationId) {
			this.userId = userId;
			this.operationId = operationId;
		}
		
		public CompositeKeyWritable() {
		}
		
		public void readFields(DataInput arg0) throws IOException {
			userId = arg0.readInt();
			operationId = arg0.readInt();
		}

		public void write(DataOutput arg0) throws IOException {
			arg0.writeInt(userId);
			arg0.writeInt(operationId);
		}

		public int compareTo(CompositeKeyWritable o) {
			int result = userId.compareTo(o.userId);
			if (result == 0)
				result = operationId.compareTo(o.operationId);
			return result;
		}

		public Integer getUserId() {
			return userId;
		}

		public void setUserId(Integer userId) {
			this.userId = userId;
		}

		public Integer getOperationId() {
			return operationId;
		}

		public void setOperationId(Integer operationId) {
			this.operationId = operationId;
		}
	}

	public class SecondarySortPartitioner extends Partitioner<CompositeKeyWritable, Text> {

		public int getPartition(CompositeKeyWritable key, Text value, int numReduceTasks) {
			return (key.getUserId().hashCode() % numReduceTasks);
		}
	}

	@SuppressWarnings("rawtypes")
	public static class SecondarySortCompositeKeySortComparator extends WritableComparator {
		public SecondarySortCompositeKeySortComparator() {
			super(CompositeKeyWritable.class, true);
		}

		public int compare(WritableComparable write1, WritableComparable write2) {
			CompositeKeyWritable key1 = (CompositeKeyWritable) write1;
			CompositeKeyWritable key2 = (CompositeKeyWritable) write2;
			int result = key1.getUserId().compareTo(key2.getUserId());
			if (result == 0)
				result = key1.getOperationId().compareTo(key2.getOperationId());
			return result;
		}
	}
	
	@SuppressWarnings("rawtypes")
	public static class SecondarySortGroupingComparator extends WritableComparator {
		public SecondarySortGroupingComparator() {
			super(CompositeKeyWritable.class, true);
		}

		
		public int compare(WritableComparable write1, WritableComparable write2) {
			CompositeKeyWritable key1 = (CompositeKeyWritable) write1;
			CompositeKeyWritable key2 = (CompositeKeyWritable) write2;
			return key1.getUserId().compareTo(key2.getUserId());
		}
	}

	/**
	 * Emits User Details, used in Reduce join 
	 * Incoming Data:	userdata.txt
	 * Out Data Format: 
	 * UserID		Address,City,State,DOB
	 */
	public static class UserDetailsMapper extends Mapper<LongWritable, Text, CompositeKeyWritable, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] data = value.toString().split(",");
			if (data.length == 10) 
				context.write(new CompositeKeyWritable(Integer.parseInt(data[0]), 0), new Text(
						data[3].concat(",").concat(data[4]).concat(",").concat(data[5]).concat(",").concat(data[9])));
		}
	}

	/**
	 * Create Friends Pair 
	 * Incoming Data: soc-LiveJournal1Adj.txt
	 * Out Data Format: 
	 * UserID 		FriendID
	 */
	public static class FriendsMapper extends Mapper<LongWritable, Text, CompositeKeyWritable, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] data = value.toString().split("\t");
			if(data.length == 2) {
				for (String friend : data[1].split(","))
					context.write(new CompositeKeyWritable(Integer.parseInt(data[0]), 1), new Text(friend));
			}
		}
	}

	/**
	 * Used for joining address Incoming Data first from UserDetailsMapper, then FriendsMapper 
	 * Out Data Format:
	 * UserID	 	FriendID,UserAddress,UserCity,UserState
	 */
	public static class Reduce1 extends Reducer<CompositeKeyWritable, Text, Text, Text> {

		public void reduce(CompositeKeyWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String city = "";
			String state = "";
			String address = "";
			for (Text text : values) {
				if (key.getOperationId() == 0) {
					address = text.toString().split(",")[0];
					city = text.toString().split(",")[1];
					state = text.toString().split(",")[2];
				} else if (key.getOperationId() == 1 && !address.isEmpty() && !city.isEmpty() && !state.isEmpty())
					context.write(new Text(key.getUserId().toString()), new Text(text.toString().concat(",")
							.concat(address).concat(",").concat(city).concat(",").concat(state)));
			}
		}
	}

	/**
	 * Inverse K-V pair of the input Incoming Data from Reduce1 
	 * Out Data Format:
	 * FriendID 	UserID,UserAddress,UserCity,UserState
	 */
	public static class InverseMapper extends Mapper<LongWritable, Text, CompositeKeyWritable, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] data = value.toString().split("\t");
			String[] values = data[1].split(",");

			context.write(new CompositeKeyWritable(Integer.parseInt(values[0]), 1), new Text(
					data[0].concat(",").concat(values[1]).concat(",").concat(values[2]).concat(",").concat(values[3])));
		}
	}

	/**
	 * Used for joining DOB Incoming Data first from UserDetailsMapper, then InverseMapper 
	 * Data Output Format: 
	 * UserID		FriendID,UserAddress,UserCity,UserState,FriendAge
	 */
	public static class Reduce2 extends Reducer<CompositeKeyWritable, Text, Text, Text> {

		public void reduce(CompositeKeyWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Integer age = 0;
			for (Text text : values) {
				if (key.getOperationId() == 0) {
					Date current = new Date();
					int Month = current.getMonth() + 1;
					int Year = current.getYear() + 1900;
					String[] cal = text.toString().split(",")[3].split("/");
					age = Year - Integer.parseInt(cal[2]);
					if (Integer.parseInt(cal[0]) > Month) {
						age--;
					} else if (Integer.parseInt(cal[0]) == Month) {
						int Day = current.getDate();
						if (Integer.parseInt(cal[1]) > Day) {
							age--;
						}
					}
				} else if (key.getOperationId() == 1 && age!=0) {
					String[] data = text.toString().split(",");
					context.write(new Text(data[0]),
							new Text(key.getUserId().toString().concat(",").concat(data[1]).concat(",").concat(data[2])
									.concat(",").concat(data[3]).concat(",").concat(age.toString())));
				}
			}
		}
	}

	/**
	 * Emits data directly from the file
	 * Incoming Data from Reduce2
	 * Out Data Format same as Reduce2: 
	 * UserID		FriendID,UserAddress,UserCity,UserState,FriendAge
	 */
	public static class EmitMapper extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			context.write(new Text(value.toString().split("\t")[0]), new Text(value.toString().split("\t")[1]));
		}
	}

	/**
	 * Finds maximum age
	 * Incoming Data from EmitMapper
	 * Out Data Format:
	 * UserID	UserAddress,UserCity,UserState,Friends-Maximum-Age
	 */
	public static class MaxAgeReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Integer maxAge=0;
			Integer age;
			boolean userData=true;
			String address="";
			String city="";
			String state="";
			for(Text text: values) {
				String[] data = text.toString().split(",");
				age = Integer.parseInt(data[4]);
				maxAge = maxAge<age?age:maxAge;
				if(userData) {
					address=data[1];
					city=data[2];
					state=data[3];
					userData=false;
				}
			}
			context.write(key, new Text(
					address.concat(",").concat(city).concat(",").concat(state).concat(",").concat(maxAge.toString())));
		}
	}
	
	/**
	 * Emit entire data by changing Key as 1
	 * Incoming Data from MaxAgeReducer
	 * Out Data Format same as MaxAgeReducer: 
	 * UserID	UserAddress,UserCity,UserState,Friends-Maximum-Age
	 */
	public static class AggregateMapper extends Mapper<LongWritable, Text, Text, Text> {

		private final static Text one = new Text(new String("1"));
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			context.write(one, value);
		}
	}
	
	/**
	 * Finds Top 10 Friends-Maximum-Age
	 * Incoming Data from AggregateMapper
	 * Out Data Format same as MaxAgeReducer:
	 * UserID	UserAddress,UserCity,UserState,Friends-Maximum-Age
	 */
	public static class Top10Reducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			HashMap<String, Integer> ageMap = new HashMap<String, Integer>();
			HashMap<String, String> dataMap = new HashMap<String, String>();
			int count = 1;
			for (Text line : values) {
				String[] fields = line.toString().split("\t");
				dataMap.put(fields[0], fields[1]);
				ageMap.put(fields[0], Integer.parseInt(fields[1].split(",")[3]));
			}
			compareValues updatedValues = new compareValues(ageMap);
			TreeMap<String, Integer> sortedAgeMap = new TreeMap<String, Integer>(updatedValues);
			sortedAgeMap.putAll(ageMap);

			for (Entry<String, Integer> entry : sortedAgeMap.entrySet()) {
				if (count <= 10) 
					context.write(new Text(entry.getKey()), new Text(dataMap.get(entry.getKey())));
				 else
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
