package me.sinu.mapr.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import me.sinu.mapr.search.support.IndexKey;
import me.sinu.mapr.search.support.IndexTuple;
import me.sinu.mapr.search.support.IndexTupleArray;
import me.sinu.mapr.search.support.SearchConstants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * A simple Inverted Index creator. Refer SearchEn project for a more evolved codebase.
 * This one just shows how to create custom Writables.
 * @author Sinu John
 * www.sinujohn.wordpress.com
 *
 */
public class InvertedIndex {

	public static class IndexMapper extends Mapper<LongWritable, Text, IndexKey, IndexTuple> {
		@Override
		protected void map(LongWritable key, Text value,
				Context context)
				throws IOException, InterruptedException {
			String filename = ((FileSplit)context.getInputSplit()).getPath().getName();
			
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			long count = 0;
			while(tokenizer.hasMoreTokens()) {
				context.write(new IndexKey(filename, tokenizer.nextToken()), new IndexTuple(key.get(), count));
				count++;
			}
		}
	}
	
	/*public static class IndexReducer extends Reducer<IndexKey, IndexTuple, Text, IndexTupleArray> {
		@Override
		protected void reduce(IndexKey key, Iterable<IndexTuple> values,
				Context context)
				throws IOException, InterruptedException {
			IndexTupleArray arr = new IndexTupleArray();
			List<IndexTuple> list = new ArrayList<IndexTuple>();
			for(IndexTuple value : values) {
				list.add(new IndexTuple(value));
			}
			arr.set(list.toArray(new IndexTuple[list.size()]));
			
			context.write(new Text(key.toString()), arr);
		}
	}*/
	
	public static class IndexReducer extends TableReducer<IndexKey, IndexTuple, Text> {
		
		@Override
		protected void reduce(IndexKey key, Iterable<IndexTuple> values,
				Context context)
				throws IOException, InterruptedException {
			
			String rowKey = key.getWord().toString() + SearchConstants.KEY_SEPARATOR + key.getFilename();
			Put put =new Put(rowKey.getBytes());
			//put.add(Bytes.toBytes(SearchConstants.COL_FAMILY), Bytes.toBytes(SearchConstants.COL_FILENAME), key.getFilename().getBytes());
			
			//String temp="";
			IndexTupleArray arr = new IndexTupleArray();
			List<IndexTuple> list = new ArrayList<IndexTuple>();
			for(IndexTuple value : values) {
				list.add(new IndexTuple(value));
				//temp += value.toString() +", ";
			}
			arr.set(list.toArray(new IndexTuple[list.size()]));
			
			//put.add(Bytes.toBytes(COL_FAMILY), Bytes.toBytes(COL_INDICES), Bytes.toBytes(temp));
			put.add(Bytes.toBytes(SearchConstants.COL_FAMILY), Bytes.toBytes(SearchConstants.COL_INDICES), WritableUtils.toByteArray(arr));
			
			context.write(new Text(key.toString()), put);
		}
	}
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
        Job job = new Job(conf, "invertedindex");
        
        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(IndexMapper.class);
        job.setReducerClass(IndexReducer.class);
        
        job.setMapOutputKeyClass(IndexKey.class);
        job.setMapOutputValueClass(IndexTuple.class);
        //job.setOutputKeyClass(Text.class);
        //job.setOutputValueClass(IndexTupleArray.class);
        
        TableMapReduceUtil.initTableReducerJob(SearchConstants.INDEX_TABLE_NAME, IndexReducer.class, job);
        
        //these are default values. no need to set explicitly
        //job.setInputFormatClass(TextInputFormat.class);
        //job.setOutputFormatClass(TextOutputFormat.class);
        /////////////////////////////////////////////////
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
