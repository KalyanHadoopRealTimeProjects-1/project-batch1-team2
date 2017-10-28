package com.eventproces.mapreduce.utils;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
	
public class PartitionUtils extends Partitioner<Text, EventProcessLog> {

	public int getPartition(Text key, EventProcessLog value, int numReduceTasks) {

		if (key.equals(new Text("SUCCESS"))) {
			return 0;
		} else {
			return 1;
		}
	}
}
