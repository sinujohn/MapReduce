package me.sinu.mapr.search.support;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 * 
 * @author Sinu John
 * www.sinujohn.wordpress.com
 *
 */
public class IndexTuple implements WritableComparable<IndexTuple> {

	private LongWritable first;
	private LongWritable second;

	public IndexTuple() {
		set(new LongWritable(), new LongWritable());
	}

	public IndexTuple(long first, long second) {
		set(new LongWritable(first), new LongWritable(second));
	}

	public IndexTuple(LongWritable first, LongWritable second) {
		set(first, second);
	}

	public IndexTuple(IndexTuple value) {
		set(new LongWritable(value.first.get()), new LongWritable(value.second.get()));
	}

	public void set(LongWritable first, LongWritable second) {
		this.first = first;
		this.second = second;
	}

	public LongWritable getFirst() {
		return first;
	}

	public LongWritable getSecond() {
		return second;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
	}

	@Override
	public int hashCode() {
		return first.hashCode() * 163 + second.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof IndexTuple) {
			IndexTuple tp = (IndexTuple) o;
			return first.equals(tp.first) && second.equals(tp.second);
		}
		return false;
	}

	@Override
	public String toString() {
		return "(" + first + "," + second + ")";
	}

	@Override
	public int compareTo(IndexTuple tp) {
		int cmp = first.compareTo(tp.first);
		if (cmp != 0) {
			return cmp;
		}
		return second.compareTo(tp.second);
	}
}