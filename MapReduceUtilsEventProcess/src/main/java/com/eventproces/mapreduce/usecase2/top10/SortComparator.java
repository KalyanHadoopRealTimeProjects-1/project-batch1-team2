package com.eventproces.mapreduce.usecase2.top10;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SortComparator extends WritableComparator {

	
	protected SortComparator() {
		 super(Text.class, true);
	 }

	 public int compare(WritableComparable o1, WritableComparable o2) {
		 Text k1 = (Text) o1;
		 Text k2 = (Text) o2;
		 LongWritable l1=new LongWritable(Long.parseLong(k1.toString()));
		 LongWritable l2=new LongWritable(Long.parseLong(k2.toString()));
		 int cmp = l1.compareTo(l2);
		 return -1 * cmp;
	 }
	
	}