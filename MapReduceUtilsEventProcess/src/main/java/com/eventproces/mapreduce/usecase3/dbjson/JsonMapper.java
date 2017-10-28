package com.eventproces.mapreduce.usecase3.dbjson;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONObject;

import com.eventproces.mapreduce.dbexport.EventProcessWritable;
import com.eventproces.mapreduce.utils.JSONUtils;

public class JsonMapper extends Mapper<LongWritable, Text, EventProcessWritable, NullWritable> {

	public void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, EventProcessWritable, NullWritable>.Context context)
					throws IOException, InterruptedException {

		JSONObject jsonObject = JSONUtils.jsonToJSONObject(value.toString());
		EventProcessWritable eventProcess = new EventProcessWritable();
		// if(jsonObject.get("dt")!=null &&jsonObject.get("country")!=null &&
		// jsonObject.get("ip")!=null && jsonObject.get("status")){
		eventProcess.setProcessdate(jsonObject.get("dt").toString());
		eventProcess.setIp(jsonObject.get("country").toString());
		eventProcess.setCountry(jsonObject.get("ip").toString());
		eventProcess.setStatus(jsonObject.get("status").toString());
		context.write(eventProcess, NullWritable.get());

	}
}
