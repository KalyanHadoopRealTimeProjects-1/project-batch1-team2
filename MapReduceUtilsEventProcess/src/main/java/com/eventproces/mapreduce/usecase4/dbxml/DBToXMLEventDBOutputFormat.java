package com.eventproces.mapreduce.usecase4.dbxml;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import com.eventproces.mapreduce.usecase4.dbjson.EventProcessWritable;
import com.eventproces.mapreduce.utils.EventProcessLog;
import com.eventproces.mapreduce.utils.OrenCustmXmlOutputFormat;

public class DBToXMLEventDBOutputFormat  extends Configured implements Tool {

	public static void main(String... args) throws Exception {

		int status = ToolRunner.run(new Configuration(), new DBToXMLEventDBOutputFormat(), args);

		System.out.println("Status: " + status);

	}

	public int run(String[] args) throws Exception {

		

		DBConfiguration.configureDB(getConf(), "com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306/workdb"+ "?user=root&password=cloudera");

		Job job = new Job(getConf());
	   // job.addFileToClassPath(new Path("/data/mysql-connector-java-5.1.37.jar"));
		
		job.setJarByClass(DBToXMLEventDBOutputFormat.class);
		job.setMapperClass(DBToXMLMapper.class);
		job.setReducerClass(DBToJsonReducer.class);

		job.setInputFormatClass(DBInputFormat.class);
		job.setOutputFormatClass(OrenCustmXmlOutputFormat.class);
	
		job.setMapOutputKeyClass(EventProcessWritable.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(EventProcessLog.class);
		//DBInputFormat.setInput(job, EventProcessWritable.class, "event_tbl", null, null, new String[]{ "processdate", "ip", "country", "status" });
		DBInputFormat.setInput(job, EventProcessWritable.class, "select * from workdb.event_tbl", "SELECT COUNT(*) FROM workdb.event_tbl");
		Path outputPath = new Path(args[0]);
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(getConf()).delete(outputPath, true);
		return job.waitForCompletion(true) ? 0 : 1;
	
	}

	
	public static class DBToXMLMapper extends Mapper<LongWritable, EventProcessWritable, EventProcessWritable, NullWritable> {

		public void map(LongWritable key, EventProcessWritable value,
				Mapper<LongWritable, EventProcessWritable, EventProcessWritable, NullWritable>.Context context)
						throws IOException, InterruptedException {
			context.write(value,NullWritable.get());
		}
	}

	public static class DBToJsonReducer extends Reducer<EventProcessWritable, NullWritable, Text, EventProcessLog> {

		protected void reduce(EventProcessWritable key, Iterable<NullWritable> arg1,
				Reducer<EventProcessWritable, NullWritable, Text, EventProcessLog>.Context context)
						throws IOException, InterruptedException {
			EventProcessLog eventProcess=new EventProcessLog();
			eventProcess.setCountry(key.getCountry());
			eventProcess.setIp(key.getIp());
			eventProcess.setProcesstimestamp(key.getProcessdate());
			eventProcess.setStatus(key.getStatus());
			context.write(new Text(),eventProcess);
			
		}
	}
}
