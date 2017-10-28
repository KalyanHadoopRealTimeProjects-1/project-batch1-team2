package com.eventproces.mapreduce.usecase4.dbjson;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;


public class DBToJsonMapper extends Mapper<LongWritable, EventProcessWritable, EventProcessWritable, NullWritable> {

	public void map(LongWritable key, EventProcessWritable value,
			Mapper<LongWritable, EventProcessWritable, EventProcessWritable, NullWritable>.Context context)
					throws IOException, InterruptedException {
	
		context.write(value,NullWritable.get());
		
		

	}
}
