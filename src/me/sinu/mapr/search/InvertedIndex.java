package me.sinu.mapr.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import me.sinu.mapr.search.support.IndexTuple;
import me.sinu.mapr.search.support.IndexTupleArray;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex {

	public static class IndexMapper extends Mapper<LongWritable, Text, Text, IndexTuple> {
		@Override
		protected void map(LongWritable key, Text value,
				Context context)
				throws IOException, InterruptedException {
			String filename = ((FileSplit)context.getInputSplit()).getPath().getName();
			
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			long count = 0;
			while(tokenizer.hasMoreTokens()) {
				context.write(new Text(tokenizer.nextToken()), new IndexTuple(key.get(), count));
				count++;
			}
		}
	}
	
	public static class IndexReducer extends Reducer<Text, IndexTuple, Text, IndexTupleArray> {
		@Override
		protected void reduce(Text key, Iterable<IndexTuple> values,
				Context context)
				throws IOException, InterruptedException {
			IndexTupleArray arr = new IndexTupleArray();
			List<IndexTuple> list = new ArrayList<IndexTuple>();
			for(IndexTuple value : values) {
				list.add(new IndexTuple(value));
			}
			arr.set(list.toArray(new IndexTuple[list.size()]));
			
			context.write(key, arr);
		}
	}
	/**
	 * @param args
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
        Job job = new Job(conf, "invertedindex");
        
        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(IndexMapper.class);
        job.setReducerClass(IndexReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IndexTuple.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IndexTupleArray.class);
        
        //these are default values. no need to set explicitly
        //job.setInputFormatClass(TextInputFormat.class);
        //job.setOutputFormatClass(TextOutputFormat.class);
        /////////////////////////////////////////////////
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
