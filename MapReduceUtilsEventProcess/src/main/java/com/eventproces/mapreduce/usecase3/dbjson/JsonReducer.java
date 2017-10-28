package com.eventproces.mapreduce.usecase3.dbjson;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class JsonReducer extends Reducer<EventProcessWritable, NullWritable, EventProcessWritable, NullWritable> {

	protected void reduce(EventProcessWritable key, Iterable<NullWritable> arg1,
			Reducer<EventProcessWritable, NullWritable, EventProcessWritable, NullWritable>.Context context)
					throws IOException, InterruptedException {

		context.write(key, NullWritable.get());

	}
}
