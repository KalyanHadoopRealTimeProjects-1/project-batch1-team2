package com.eventproces.mapreduce.utils;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class OrenCustmXmlOutputFormat extends FileOutputFormat<Text, EventProcessLog> {

	/*
	<kalyan>
	<record>
		<dt>2013-11-24T19:08:21</dt>
		<status>SUCCESS</status>
		<ip>189.89.146.110</ip>
		<country>FR</country>
	</record>
	</kalyan>*/

	
	
	
	public RecordWriter<Text, EventProcessLog> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {

		String file_extension = ".xml";

		Path file = getDefaultWorkFile(job, file_extension);

		FileSystem fs = file.getFileSystem(job.getConfiguration());

		FSDataOutputStream fileOut = fs.create(file, false);
	
		return new OrenCustXMLRecordWriter(fileOut);
		
	}

	public static class OrenCustXMLRecordWriter extends RecordWriter<Text, EventProcessLog> {

		private DataOutputStream out;

		public OrenCustXMLRecordWriter(DataOutputStream out) throws IOException
		{
			this.out = out;
			out.writeBytes("<kalyan>\n");
		}
		
		private void writeStyle(String xml_tag, String tag_value) throws IOException {

			out.writeBytes("<" + xml_tag + ">" + tag_value + "</" + xml_tag + ">\n");

		}

		
		public void write(Text key, EventProcessLog value) throws IOException, InterruptedException {
			/*
			<dt>2013-11-24T19:08:21</dt>
			<status>SUCCESS</status>
			<ip>189.89.146.110</ip>
			<country>FR</country>*/
			out.writeBytes("<record>\n");
			this.writeStyle("dt", value.getProcesstimestamp());
			this.writeStyle("status", value.getStatus());
			this.writeStyle("ip", value.getIp());
			this.writeStyle("country", value.getCountry());
			out.writeBytes("</record>\n");

		}

		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			try {
				out.writeBytes("</kalyan>\n");
			} finally {
				out.close();
			}

		
		}

		
		
	}
}
