package me.sinu.mapr.translation;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * A program which shows translations of a word
 * 
 * @author sinu
 * www.sinujohn.wordpress.com
 *
 */
public class Translation {

	public static class TMapper extends Mapper<Text, Text, Text, Text> {
		
		@Override
		protected void map(Text key, Text value,
				Context context)
				throws IOException, InterruptedException {
			
			StringTokenizer tokenizer = new StringTokenizer(value.toString(), ",");
			while(tokenizer.hasMoreTokens()) {
				String val = tokenizer.nextToken().trim();
				if(!val.isEmpty()) {
					context.write(key, new Text(val));
				}
			}
		}
	}
	
	public static class TReducer extends Reducer<Text, Text, Text, Text> {
		
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Context context)
				throws IOException, InterruptedException {
			
			StringBuilder sb = new StringBuilder();
			for(Text value : values) {
				sb.append(value.toString() + " | ");
			}
			
			context.write(key, new Text(sb.toString()));			
		}
	}
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		//Let us set the separator for class KeyValueTextInputFormat as SPACE
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", " ");
        Job job = new Job(conf, "translation");
        
        job.setJarByClass(Translation.class);
        job.setMapperClass(TMapper.class);
        job.setReducerClass(TReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        //here we have our input file in such a way that first word is the key and remaining is the value
        //by default key and value is separated by TAB
        //we set it to SPACE by mentioning it in the Configuration as shown above
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        
	}

}
