package com.eventproces.mapreduce.usecase3.dbjson;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;

import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.util.Tool;

import org.apache.hadoop.util.ToolRunner;

public class JSONEventDBOutputFormatDriver extends Configured implements Tool {

	public static void main(String... args) throws Exception {

		int status = ToolRunner.run(new Configuration(), new JSONEventDBOutputFormatDriver(), args);

		System.out.println("Status: " + status);

	}

	public int run(String[] args) throws Exception {

		DBConfiguration.configureDB(getConf(), "com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306/workdb"

		+ "?user=root&password=cloudera");

		Job job = new Job(getConf());

		// job.addFileToClassPath(new
		// Path("/data/lib/mysql-connector-java-5.1.14.jar"));

		job.setJarByClass(JSONEventDBOutputFormatDriver.class);

		job.setMapperClass(JsonMapper.class);

		job.setReducerClass(JsonReducer.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setOutputFormatClass(DBOutputFormat.class);

		job.setMapOutputKeyClass(EventProcessWritable.class);

		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(EventProcessWritable.class);

		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));

		DBOutputFormat.setOutput(job, "EVENT_TBL", EventProcessWritable.fields);

		return job.waitForCompletion(true) ? 0 : 1;

	}

}
