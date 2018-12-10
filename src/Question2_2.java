import java.io.IOException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.google.common.collect.MinMaxPriorityQueue;

public class Question2_2 {

	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] flickerEntry = value.toString().split("\t");	
			String longitude= flickerEntry[10];
			String latitude = flickerEntry[11];
			
			// Not all fields may have a value
			Country country = null;
			if ( (latitude != null && !latitude.isEmpty()) && (longitude!=null && !longitude.isEmpty()) ) {
				
				country = Country.getCountryAt(Double.parseDouble(latitude), Double.parseDouble(longitude));
			}
			
			// (comma-separated) and adding the two tag (user and robot)
			if (country != null) {
				
				String[] tags = (URLDecoder.decode(flickerEntry[8])+","+ URLDecoder.decode(flickerEntry[9])).split(",");
				for (String tag : tags) {

					context.write(new Text(country.toString()), new Text(tag));
				}
			}			
		}
	}
	
	public static class MyCombiner extends Reducer<Text, StringAndInt, Text, StringAndInt> {
		@Override
		protected void reduce(Text key, Iterable<StringAndInt> values, Context context) throws IOException, InterruptedException {
			
			// Count the tag occurence for a given country (copy of start of previous reduce iteration)
			HashMap<String, Integer> tagTotalPerCountry = new HashMap<String, Integer>();
			for (StringAndInt oneTag : values) {
				
				if (tagTotalPerCountry.containsKey(oneTag.tag)) {
					
					Integer totalOccurence = tagTotalPerCountry.get(oneTag) + oneTag.nbOccurence;
					tagTotalPerCountry.put(oneTag.tag, totalOccurence);
				} else {
					
					tagTotalPerCountry.put(oneTag.tag, oneTag.nbOccurence);
				}
			}

			// Output all tag occurence for a country
			for (String tagName : tagTotalPerCountry.keySet()){
				context.write(key, new StringAndInt(tagName, tagTotalPerCountry.get(tagName)));
			}
		}
	}

	public static class MyReducer extends Reducer<Text, StringAndInt, Text, Text> {
	    private IntWritable result = new IntWritable();

		@Override
		protected void reduce(Text key, Iterable<StringAndInt> values, Context context) throws IOException, InterruptedException {
						
			MinMaxPriorityQueue<StringAndInt> allTag = MinMaxPriorityQueue.maximumSize(context.getConfiguration().getInt("kParam", 1)).create();
			
			HashMap<String, Integer> map = new HashMap<String, Integer>();

			for (StringAndInt tagTotalPerCountry : values) {			
				if (map.containsKey(tagTotalPerCountry.tag)) {
					
					Integer totalOccurence = map.get(tagTotalPerCountry) + tagTotalPerCountry.nbOccurence;
					map.put(tagTotalPerCountry.tag, totalOccurence);
				} else {
					
					map.put(tagTotalPerCountry.tag, tagTotalPerCountry.nbOccurence);
				}	
			}
			
			for (String stringAndInt : map.keySet()) {
				
				allTag.add(new StringAndInt(stringAndInt,map.get(stringAndInt)));
			}

			for (int i=0; i < context.getConfiguration().getInt("kParam", 1); i++){
				StringAndInt tagToAdd = allTag.pollFirst();				
				context.write(key, new Text(tagToAdd.tag+" "+tagToAdd.nbOccurence));
			}	
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String input = otherArgs[0];
		String output = otherArgs[1];
		conf.setInt("kParam", Integer.parseInt(otherArgs[2]));
		
		Job job = Job.getInstance(conf, "Question2_2");
		job.setJarByClass(Question2_1.class);

		job.setMapperClass(MyMapper.class);
		job.setCombinerClass(MyCombiner.class);
		job.setReducerClass(MyReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
				
		FileInputFormat.addInputPath(job, new Path(input));		
		job.setInputFormatClass(TextInputFormat.class);
		
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputFormatClass(TextOutputFormat.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
