package com.eventproces.mapreduce.usecase3.dbxml;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

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
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.eventproces.mapreduce.usecase3.dbjson.EventProcessWritable;

public class XMLToDBOutputFormat extends Configured implements Tool {

	public static void main(String... args) throws Exception {

		int status = ToolRunner.run(new Configuration(), new XMLToDBOutputFormat(), args);

		System.out.println("Status: " + status);

	}

	public int run(String[] args) throws Exception {

		DBConfiguration.configureDB(getConf(), "com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306/workdb"

		+ "?user=root&password=cloudera");

		Job job = new Job(getConf());

		// job.addFileToClassPath(new
		// Path("/data/lib/mysql-connector-java-5.1.14.jar"));

		job.setJarByClass(XMLToDBOutputFormat.class);

		job.setMapperClass(XMLToDBInputFomratMapper.class);

		job.setReducerClass(XMLToDBInputFomratReducer.class);

		job.setInputFormatClass(XmlInputFormat.class);

		job.setOutputFormatClass(DBOutputFormat.class);

		job.setMapOutputKeyClass(EventProcessWritable.class);

		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(EventProcessWritable.class);

		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));

		DBOutputFormat.setOutput(job, "EVENT_TBL", EventProcessWritable.fields);

		return job.waitForCompletion(true) ? 0 : 1;

	}




	
	public static class XMLToDBInputFomratMapper extends Mapper<LongWritable, Text, EventProcessWritable, NullWritable> {

		public void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, EventProcessWritable, NullWritable>.Context context)
						throws IOException, InterruptedException {

		
			try {
				InputStream is = new ByteArrayInputStream(value.toString().getBytes());
				DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
				DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
				Document doc = dBuilder.parse(is);
				doc.getDocumentElement().normalize();
				NodeList nList = doc.getElementsByTagName("record");
				for (int temp = 0; temp < nList.getLength(); temp++) {
					Node nNode = nList.item(temp);
					if (nNode.getNodeType() == Node.ELEMENT_NODE) {
						Element eElement = (Element) nNode;
						String date = eElement.getElementsByTagName("dt").item(0).getTextContent();
						String status = eElement.getElementsByTagName("status").item(0).getTextContent();
						String ip = eElement.getElementsByTagName("ip").item(0).getTextContent();
						String country = eElement.getElementsByTagName("country").item(0).getTextContent();
						
						EventProcessWritable eventProcess = new EventProcessWritable();
						// if(jsonObject.get("dt")!=null &&jsonObject.get("country")!=null &&
						// jsonObject.get("ip")!=null && jsonObject.get("status")){
						eventProcess.setProcessdate(date);
						eventProcess.setIp(ip);
						eventProcess.setCountry(country);
						eventProcess.setStatus(status);
						context.write(eventProcess, NullWritable.get());
					}
				}
			} catch (Exception e) {

			}
		}
			
		
		}
		
	public  static class XMLToDBInputFomratReducer extends Reducer<EventProcessWritable, NullWritable, EventProcessWritable, NullWritable> {

		protected void reduce(EventProcessWritable key, Iterable<NullWritable> arg1,
				Reducer<EventProcessWritable, NullWritable, EventProcessWritable, NullWritable>.Context context)
						throws IOException, InterruptedException {

			context.write(key, NullWritable.get());

		}
	}


}
