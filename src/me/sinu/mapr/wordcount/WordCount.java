package me.sinu.mapr.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

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

/**
 * The HelloWorld program for MapReduce!
 * 
 * @author sinu
 * www.sinujohn.wordpress.com
 *
 */
public class WordCount {

	public static class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
		private Text word = new Text();
		private static final IntWritable one = new IntWritable(1);
		
		@Override
		protected void map(LongWritable key, Text value,
				Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while(tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				context.write(word, one);
			}
		}
	}
	
	public static class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for(IntWritable i : values) {
				sum += i.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
        Job job = new Job(conf, "wordcount");
        
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        //these are default values. no need to set explicitly
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        /////////////////////////////////////////////////
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
