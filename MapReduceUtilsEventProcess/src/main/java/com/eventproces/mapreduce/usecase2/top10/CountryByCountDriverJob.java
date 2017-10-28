package com.eventproces.mapreduce.usecase2.top10;

import java.io.IOException;

import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class CountryByCountDriverJob implements Tool{


	private Configuration conf;
	
	public Configuration getConf() {
	
		return conf;
	}

	public void setConf(Configuration arg0) {
		this.conf=arg0;
		
	}


	
	public static class CountryByCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

		private Text country = new Text();
		Logger LOGGER = Logger.getLogger(CountryByCountMapper.class.getName());

		public void map(LongWritable key, Text value,Mapper<LongWritable, Text, Text,IntWritable>.Context context)throws IOException, InterruptedException {

			LOGGER.info("CSVMapper class stared here");
			String[] sps = value.toString().split("\\|");
			country.set(sps[3]);
			EventProcessLog eventProcess = new EventProcessLog();
			eventProcess.setProcesstimestamp(sps[0]);
			eventProcess.setIp(sps[1]);
			eventProcess.setCountry(sps[2]);
			eventProcess.setStatus(sps[3]);
			if("ERROR".equalsIgnoreCase(eventProcess.getStatus())){
			context.write(new Text(sps[2]), new IntWritable(1));
			}
			LOGGER.info("CSVMapper class ended here");
		}

	}

	public static class CountryByCountReducer extends Reducer<Text,IntWritable, Text, Text>{
		
		protected void reduce(Text key, Iterable<IntWritable> arg1,
				Reducer<Text, IntWritable, Text, Text>.Context context)
				throws IOException, InterruptedException {
			   int sum=0;
			    for(IntWritable cnt:arg1){
			    	sum=sum+cnt.get();
			    }
			    context.write(key, new Text(String.valueOf(sum)));
			    
		}	
		
	}
	
	public int run(String[] args) throws Exception {

		// initializing the job configuration
		Job wordCountJob = new Job(getConf());

		// setting the job name
		wordCountJob.setJobName("Iwinner IT TOP10 Job");

		// to call this as a jar
		wordCountJob.setJarByClass(this.getClass());
	
			
		// setting custom mapper class
		wordCountJob.setMapperClass(CountryByCountMapper.class);
	
		// setting custom reducer class
		wordCountJob.setReducerClass(CountryByCountReducer.class);

		// setting custom combinder class
		// wordCountJob.setCombinerClass(WordCountReducer.class);

		// setting no of reducers
		// wordCountJob.setNumReduceTasks(26);

		// setting custom partitioner class
		// wordCountJob.setPartitionerClass(WordCountPartitioner.class);

		// setting mapper output key class: K2		
		wordCountJob.setMapOutputKeyClass(Text.class);
		// setting mapper output value class: V2
		wordCountJob.setMapOutputValueClass(IntWritable.class);
		// setting final output key class: K3	
		wordCountJob.setOutputKeyClass(Text.class);
		// setting final output value class: V3
		wordCountJob.setOutputValueClass(Text.class);

		// setting the input format class ,i.e for K1, V1
		wordCountJob.setInputFormatClass(TextInputFormat.class);

		// setting the output format class
		wordCountJob.setOutputFormatClass(TextOutputFormat.class);

		// setting the input file path
		FileInputFormat.addInputPath(wordCountJob, new Path(args[0]));

		// setting the output folder path
		FileOutputFormat.setOutputPath(wordCountJob, new Path(args[1]));

		Path outputpath = new Path(args[1]);
		// delete the output folder if exists
		outputpath.getFileSystem(conf).delete(outputpath, true);

		// to execute the job and return the status
		return wordCountJob.waitForCompletion(true) ? 0 : -1;

	}

	
	public static void main(String[] args) {
		
		try{
			Configuration conf = new Configuration();
			conf.set("limit", "20");
		int exitCode = ToolRunner.run(conf,	new CountryByCountDriverJob(), args);
		
		System.out.println("Status Code=>"+exitCode);
		
		System.exit(exitCode);
		}catch(Exception e){
				
		}
	}
}

