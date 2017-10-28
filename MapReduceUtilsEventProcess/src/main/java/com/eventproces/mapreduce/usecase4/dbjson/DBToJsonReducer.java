package com.eventproces.mapreduce.usecase4.dbjson;

import java.io.IOException;


import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.google.gson.GsonBuilder;

public class DBToJsonReducer extends Reducer<EventProcessWritable, NullWritable, Text, NullWritable> {

	protected void reduce(EventProcessWritable key, Iterable<NullWritable> arg1,
			Reducer<EventProcessWritable, NullWritable, Text, NullWritable>.Context context)
					throws IOException, InterruptedException {
		try {
			context.write(new Text(new JSONObject(new GsonBuilder().create().toJson(key)).toString()), NullWritable.get());
		} catch (JSONException e) {

		}
		
	}
}
