package me.sinu.mapr.search.support;

import java.util.Arrays;

import org.apache.hadoop.io.ArrayWritable;

public class IndexTupleArray extends ArrayWritable {

	public IndexTupleArray() {
		super(IndexTuple.class);
	}
	
	@Override
	public String toString() {
		return Arrays.toString(get());
	}

}
