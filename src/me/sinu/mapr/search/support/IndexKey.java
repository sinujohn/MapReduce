package me.sinu.mapr.search.support;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * 
 * @author Sinu John
 * www.sinujohn.wordpress.com
 *
 */
public class IndexKey implements WritableComparable<IndexKey> {

	private Text filename;
	private Text word;

	public IndexKey() {
		set(new Text(), new Text());
	}

	public IndexKey(String filename, String word) {
		set(new Text(filename), new Text(word));
	}

	public IndexKey(Text filename, Text word) {
		set(filename, word);
	}

	public IndexKey(IndexKey value) {
		set(new Text(value.filename), new Text(value.word));
	}

	public void set(Text filename, Text word) {
		this.filename = filename;
		this.word = word;
	}

	public Text getFilename() {
		return filename;
	}

	public Text getWord() {
		return word;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		filename.write(out);
		word.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		filename.readFields(in);
		word.readFields(in);
	}

	@Override
	public int hashCode() {
		return filename.hashCode() * 163 + word.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof IndexKey) {
			IndexKey tp = (IndexKey) o;
			return filename.equals(tp.filename) && word.equals(tp.word);
		}
		return false;
	}

	@Override
	public String toString() {
		return "(" + filename + ":" + word + ")";
	}

	@Override
	public int compareTo(IndexKey tp) {
		int cmp = filename.compareTo(tp.filename);
		if (cmp != 0) {
			return cmp;
		}
		return word.compareTo(tp.word);
	}
}