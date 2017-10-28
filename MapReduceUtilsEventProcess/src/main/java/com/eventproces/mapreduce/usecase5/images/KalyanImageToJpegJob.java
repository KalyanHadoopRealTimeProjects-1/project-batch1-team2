package com.eventproces.mapreduce.usecase5.images;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class KalyanImageToJpegJob implements Tool {

	// Declare Configuration Object
	private Configuration conf;

	public Configuration getConf() {
		return conf; // Getting the configuration
	}

	public void setConf(Configuration conf) {
		this.conf = conf; // Setting the configuration
	}

	public int run(String[] args) throws Exception {
		// initializing the job object with configuration
		Job job = new Job(getConf());

		// setting the job name
		job.setJobName("Orien IT Image to Jpeg Job");

		// Set the Jar by finding where a given class came from
		job.setJarByClass(this.getClass());

		// setting custom mapper class
		job.setMapperClass(ImageMapper.class);

		// setting custom reducer class
		job.setReducerClass(ImageReducer.class);

		// setting mapper output key class: K2
		job.setMapOutputKeyClass(Text.class);

		// setting mapper output value class: V2
		job.setMapOutputValueClass(KalyanImageWritable.class);

		// setting final output key class: K3
		job.setOutputKeyClass(Text.class);

		// setting final output value class: V3
		job.setOutputValueClass(KalyanImageWritable.class);

		// setting the input format class ,i.e for K1, V1
		job.setInputFormatClass(KalyanImageInputFormat.class);

		// setting the output format class
		job.setOutputFormatClass(KalyanImageOutputFormat.class);

		// define the input & output paths
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);

		// setting the input path
		FileInputFormat.addInputPath(job, input);

		// setting the output path
		FileOutputFormat.setOutputPath(job, output);

		// delete the output path if exists
		output.getFileSystem(conf).delete(output, true);

		// to execute the job and return the status
		return job.waitForCompletion(true) ? 0 : -1;
	}

	public static void main(String[] args) throws Exception {
		// if `status == 0` then `Job Success`
		// if `status == -1` then `Job Failure`
		int status = ToolRunner.run(new Configuration(), new KalyanImageToJpegJob(), args);

		System.out.println("Job Status: " + status);
	}

	public static class ImageMapper extends Mapper<Text, KalyanImageWritable, Text, KalyanImageWritable> {
		@Override
		public void map(Text key, KalyanImageWritable value, Context context) throws IOException, InterruptedException {
			context.write(key, value);
		}
	}

	public static class ImageReducer extends Reducer<Text, KalyanImageWritable, Text, KalyanImageWritable> {
		@Override
		public void reduce(Text key, Iterable<KalyanImageWritable> values, Context context) throws IOException, InterruptedException {
			for (KalyanImageWritable val : values) {
				context.write(key, val);
			}
		}
	}
}
