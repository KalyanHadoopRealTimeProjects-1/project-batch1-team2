package com.eventproces.mapreduce.usecase4.dbjson;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class DBToJSONEventDBInputFomrat extends Configured implements Tool {

	public static void main(String... args) throws Exception {

		int status = ToolRunner.run(new Configuration(), new DBToJSONEventDBInputFomrat(), args);

		System.out.println("Status: " + status);

	}

	public int run(String[] args) throws Exception {

		

		DBConfiguration.configureDB(getConf(), "com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306/workdb"+ "?user=root&password=cloudera");

		Job job = new Job(getConf());
	  //  job.addFileToClassPath(new Path("/data/mysql-connector-java-5.1.37.jar"));
		
		job.setJarByClass(DBToJSONEventDBInputFomrat.class);
		job.setMapperClass(DBToJsonMapper.class);
		job.setReducerClass(DBToJsonReducer.class);
		job.setInputFormatClass(DBInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(EventProcessWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		
		
	
		//DBInputFormat.setInput(job, EventProcessWritable.class, "event_tbl", null, null, new String[]{ "processdate", "ip", "country", "status" });

		DBInputFormat.setInput(job, EventProcessWritable.class, "select * from workdb.event_tbl", "SELECT COUNT(*) FROM workdb.event_tbl");
		
		
		Path outputPath = new Path(args[0]);

		FileOutputFormat.setOutputPath(job, outputPath);

		outputPath.getFileSystem(getConf()).delete(outputPath, true);

		return job.waitForCompletion(true) ? 0 : 1;
	
	}

}
