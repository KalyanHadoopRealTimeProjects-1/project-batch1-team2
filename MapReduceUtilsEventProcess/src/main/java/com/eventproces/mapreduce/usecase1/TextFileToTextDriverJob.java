package com.eventproces.mapreduce.usecase1;

import java.io.IOException;

import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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

import com.eventproces.mapreduce.utils.EventConstants;
import com.eventproces.mapreduce.utils.EventProcessLog;

public class TextFileToTextDriverJob implements Tool{
	
	
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
		job.setJarByClass(TextFileToTextDriverJob.class);
		
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(TextToTextMapper.class);
		job.setReducerClass(TextToTextReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(EventProcessLog.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;

		
	}

	
	public static class TextToTextMapper	extends	org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, EventProcessLog> {

		private Text country = new Text();
		Logger LOGGER = Logger.getLogger(TextToTextMapper.class.getName());

		public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, EventProcessLog>.Context context)
				throws IOException, InterruptedException {
			LOGGER.info("CSVMapper class stared here");

			String[] sps = value.toString().split(EventConstants.CSV_DELIMITER);

			country.set(sps[3]);

			EventProcessLog eventProcess = new EventProcessLog();

			eventProcess.setProcesstimestamp(sps[0]);
			eventProcess.setIp(sps[1]);
			eventProcess.setCountry(sps[2]);
			eventProcess.setStatus(sps[3]);

			context.write(new Text(sps[2]), eventProcess);

			LOGGER.info("CSVMapper class ended here");
		}

	}

	public static class TextToTextReducer extends Reducer<Text, EventProcessLog, Text, Text> {

		Logger LOGGER = Logger.getLogger(TextToTextReducer.class.getName());
		private MultipleOutputs<Text, Text> mos;

		public void setup(Context context) throws IOException, InterruptedException {
			mos = new MultipleOutputs<Text, Text>(context);

		}

		public void reduce(Text key, Iterable<EventProcessLog> values,
				Reducer<Text, EventProcessLog, Text, Text>.Context context)
				throws IOException, InterruptedException {

			LOGGER.info("CSVReducer reduce method is stared here ");
			for (EventProcessLog value : values) {
				/*
				 * R 2013-11-26T19:08:21|88.108.201.57|FR|SUCCESS FR
				 * 2013-11-26T18:08:21|28.44.152.102|FR|ERROR
				 */
				// if("SUCCESS".getvalue.getStatus()

				if ("SUCCESS".equalsIgnoreCase(value.getStatus())) {
					mos.write(key, new Text(value.toString()),new Text(key+"_SUCCESS").toString());
				} else if ("ERROR".equalsIgnoreCase(value.getStatus())) {
					mos.write(key, new Text(value.toString()),new Text(key+"_ERROR").toString());
				} else {
					mos.write(key, new Text(value.toString()), new Text(key).toString());
				}
				
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
	int textStatus = ToolRunner.run( new Configuration(),	new TextFileToTextDriverJob(), args);
	System.out.println("textStatus Code=>"+textStatus);
	}catch(Exception e){
		
	}
	
	
	
}	
}
