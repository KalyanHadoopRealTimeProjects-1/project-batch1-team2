package com.eventproces.mapreduce.usecase1;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

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
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.eventproces.mapreduce.usecase1.JSONFileToJSONDriverJob.JSONToJSONReducer;
import com.eventproces.mapreduce.utils.EventProcessLog;
import com.eventproces.mapreduce.utils.OrenCustmXmlInputFormat;
import com.eventproces.mapreduce.utils.OrenCustmXmlOutputFormat;

public class XmlFileToXmlDriverJob implements Tool {

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
		job.setJobName("XML TO XML MultipleFile Output Format POC");
		job.setJarByClass(JSONFileToJSONDriverJob.class);
		
		job.setInputFormatClass(OrenCustmXmlInputFormat.class);
		LazyOutputFormat.setOutputFormatClass(job, OrenCustmXmlOutputFormat.class);
		
		/*MultipleOutputs.addNamedOutput(job,"SUCCESS", OrenCustmXmlOutputFormat.class,Text.class,EventProcessLog.class);
		MultipleOutputs.addNamedOutput(job,"ERROR", OrenCustmXmlOutputFormat.class,Text.class,EventProcessLog.class);
		MultipleOutputs.addNamedOutput(job,"NA", OrenCustmXmlOutputFormat.class,Text.class,EventProcessLog.class);
		*/FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(XMLToXMLMapper.class);
		job.setReducerClass(XMLToXMLReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(EventProcessLog.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(EventProcessLog.class);
		
		
		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;

		
		}

	
	public static void main(String[] args) {
		try{
			Configuration conf=new Configuration();
			conf.set("xmlinput.start", "<kalyan>");
			conf.set("xmlinput.end", "</kalyan>");

			int textStatus = ToolRunner.run( conf,new XmlFileToXmlDriverJob(), args);
			System.out.println("textStatus Code=>"+textStatus);
			
		}catch(Exception e){
				
			}
			

	}
	
	
	public static class XMLToXMLMapper extends Mapper<LongWritable, Text, Text, EventProcessLog> {

		/*
		<kalyan>
		<record>
			<dt>2013-11-24T19:08:21</dt>
			<status>SUCCESS</status>
			<ip>189.89.146.110</ip>
			<country>FR</country>
		</record>
		</kalyan>
		
		
		*/
		
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, EventProcessLog>.Context context)
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

						EventProcessLog eventProcess = new EventProcessLog();
						eventProcess.setProcesstimestamp(date);
						eventProcess.setIp(ip);
						eventProcess.setCountry(country);
						eventProcess.setStatus(status);
						context.write(new Text(country), eventProcess);
					}
				}
			} catch (Exception e) {

			}
		}
		}

	
	
	public static class XMLToXMLReducer extends Reducer<Text, EventProcessLog, Text, EventProcessLog> {

		Logger LOGGER = Logger.getLogger(JSONToJSONReducer.class.getName());
		private MultipleOutputs<Text, EventProcessLog> mos;

		public void setup(Context context) throws IOException, InterruptedException {
			mos = new MultipleOutputs<Text, EventProcessLog>(context);

		}	

		public void reduce(Text key, Iterable<EventProcessLog> values,
				Reducer<Text, EventProcessLog, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {

			try{
			LOGGER.info("CSVReducer reduce method is stared here ");
			for (EventProcessLog value : values) {
            if ("SUCCESS".equalsIgnoreCase(value.getStatus())) {
					mos.write("SUCCESS",key ,value);
			} else if ("ERROR".equalsIgnoreCase(value.getStatus())) {
				mos.write("ERROR",key ,value);
			} else {
				mos.write("NA",key ,value);
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
}


