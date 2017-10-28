/*package com.eventproces.mapreduce.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.eventproces.mapreduce.use3.dbxml.XMLMultipleMapper;
import com.eventproces.mapreduce.use3.dbxml.XmlInputFormat;

public class ExampleRefer extends Configured implements Tool {

		public int run(String[] args) throws Exception {

			if (args.length != 2) {
				System.out
						.printf("Two parameters are required for DriverFormatMultiOutput- <input dir> <output dir>\n");
				return -1;
			}

		
			
			//get the FileSystem, you will need to initialize it properly
			FileSystem fs= FileSystem.get(getConf()); 
			//get the FileStatus list from given dir
			FileStatus[] status_list = fs.listStatus(new Path(args[0]));
			if(status_list != null){
			    for(FileStatus status : status_list){
			    	
			    	if( status.getPath().getName().endsWith(".txt")){

			    		Job job = new Job(getConf());
						
			    		job.setJobName("MultipleOutputs TEXT WORK");

						job.setJarByClass(ExampleRefer.class);
						
			    		
			    		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
						FileInputFormat.setInputPaths(job, status.getPath());
						FileOutputFormat.setOutputPath(job, new Path(args[1]+"/csv/"));
						
						job.setMapperClass(MutlipleMapper.class);
						job.setReducerClass(MutlipleReducer.class);
						
						job.setInputFormatClass(TextInputFormat.class);
				
							
						job.setMapOutputKeyClass(Text.class);
						job.setMapOutputValueClass(EventProcessLog.class);
						
						job.setOutputKeyClass(Text.class);
						job.setOutputValueClass(Text.class);

						 boolean success = job.waitForCompletion(true);
						 
			    	}else if( status.getPath().getName().endsWith(".xml")){

			    		getConf().set("xmlinput.start", "<kalyan>");
			    		getConf().set("xmlinput.end", "</kalyan>");

			    		
			    		Job job = new Job(getConf());
						
			    		
			    		job.setJobName("MultipleOutputs XML example");

						job.setJarByClass(ExampleRefer.class);
			    		

			    		

			    		  
			    		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
						
			    		FileInputFormat.setInputPaths(job, status.getPath());
						FileOutputFormat.setOutputPath(job, new Path(args[1]+"/xml/"));
	
						
						job.setInputFormatClass(XmlInputFormat.class);
				
							
						job.setMapperClass(XMLMultipleMapper.class);
						//job.setReducerClass(MutlipleReducer.class);	
				
							
						job.setMapOutputKeyClass(Text.class);
						job.setMapOutputValueClass(EventProcessLog.class);
						
						job.setOutputKeyClass(Text.class);
						job.setOutputValueClass(Text.class);


						 boolean success = job.waitForCompletion(true);
							
			    	}
			    }
			}
			
			
			
			
				//		job.setNumReduceTasks(4);

			//boolean success = job.waitForCompletion(true);
			return 1;
		}

		public static void main(String[] args) throws Exception {
			int exitCode = ToolRunner.run(new Configuration(),
					new ExampleRefer(), args);
			System.exit(exitCode);
		}
	}*/