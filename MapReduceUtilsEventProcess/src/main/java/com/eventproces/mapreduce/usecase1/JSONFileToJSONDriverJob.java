package com.eventproces.mapreduce.usecase1;

import java.io.IOException;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.simple.JSONObject;

import com.eventproces.mapreduce.utils.EventProcessLog;
import com.eventproces.mapreduce.utils.JSONUtils;
import com.google.gson.GsonBuilder;

public class JSONFileToJSONDriverJob implements Tool{
	
	
	private Configuration conf;

	public void setConf(Configuration conf) {
		this.conf=conf;
	}

	public Configuration getConf() {
		return conf;
	}

	public int run(String[] args) throws Exception {



		if (args.length != 2) {
			System.out
					.printf("Two parameters are required for DriverFormatMultiOutput- <input dir> <output dir>\n");
			return -1;
		}

		Job job = new Job(getConf());
		job.setJobName("MultipleOutputs example");
		job.setJarByClass(JSONFileToJSONDriverJob.class);
		
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(JSONToJSONMapper.class);
		job.setReducerClass(JSONToJSONReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(EventProcessLog.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;

		
	}

	
	public static class JSONToJSONMapper	extends	org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, EventProcessLog> {

		private Text country = new Text();
		Logger LOGGER = Logger.getLogger(JSONToJSONMapper.class.getName());

		public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, EventProcessLog>.Context context)
				throws IOException, InterruptedException {
			LOGGER.info("CSVMapper class stared here");

			
			JSONObject jsonObject = JSONUtils.jsonToJSONObject(value.toString());
			EventProcessLog eventProcess = new EventProcessLog();

            eventProcess.setProcesstimestamp(jsonObject.get("dt").toString());
			eventProcess.setIp(jsonObject.get("country").toString());
			eventProcess.setCountry(jsonObject.get("ip").toString());
			eventProcess.setStatus(jsonObject.get("status").toString());

			context.write(new Text(jsonObject.get("ip").toString()), eventProcess);
			LOGGER.info("CSVMapper class ended here");
		}

	}

	public static class JSONToJSONReducer extends Reducer<Text, EventProcessLog, Text, NullWritable> {

		Logger LOGGER = Logger.getLogger(JSONToJSONReducer.class.getName());
		private MultipleOutputs<Text, NullWritable> mos;

		public void setup(Context context) throws IOException, InterruptedException {
			mos = new MultipleOutputs<Text, NullWritable>(context);

		}	

		public void reduce(Text key, Iterable<EventProcessLog> values,
				Reducer<Text, EventProcessLog, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {

			try{
			LOGGER.info("CSVReducer reduce method is stared here ");
			for (EventProcessLog value : values) {
            if ("SUCCESS".equalsIgnoreCase(value.getStatus())) {
					mos.write(new Text(new org.codehaus.jettison.json.JSONObject(new GsonBuilder().create().toJson(value.toString())).toString()),NullWritable.get(),new Text(key+"_SUCCESS").toString());
				} else if ("ERROR".equalsIgnoreCase(value.getStatus())) {
					mos.write(new Text(new org.codehaus.jettison.json.JSONObject(new GsonBuilder().create().toJson(value.toString())).toString()),NullWritable.get(),new Text(key+"_ERROR").toString());
				} else {
					mos.write(new Text(new org.codehaus.jettison.json.JSONObject(new GsonBuilder().create().toJson(value.toString())).toString()),NullWritable.get(), new Text(key).toString());
				}
			
			}

			}catch(Exception e){
				
			}
			LOGGER.info("CSVReducer reduce method is ended here ");
		}

		public void cleanup(Context context) throws IOException,
				InterruptedException {
			mos.close();
		}
	}

public static void main(String[] args) {
	try{
	int textStatus = ToolRunner.run( new Configuration(),	new JSONFileToJSONDriverJob(), args);
	System.out.println("textStatus Code=>"+textStatus);
	}catch(Exception e){
		
	}
	
	
	
}	
}
