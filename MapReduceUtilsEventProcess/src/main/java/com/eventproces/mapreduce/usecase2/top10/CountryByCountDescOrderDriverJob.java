package com.eventproces.mapreduce.usecase2.top10;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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


public class CountryByCountDescOrderDriverJob implements Tool{


	public static class CountryByCountDescOrderMapper extends Mapper<LongWritable, Text, Text, Text>{
        protected void map(LongWritable key, Text value,
        		Mapper<LongWritable, Text, Text, Text>.Context context)
        		throws IOException, InterruptedException {
        	String values[]=value.toString().split("	");
        	String keyText=values[0];
        	String valueText=values[1];
        	context.write(new Text(valueText),new Text(keyText));
        }
	}
	

	public static class CountryByCountDescOrderReducer extends Reducer<Text, Text, Text, Text>{
		
		int limit;
		int count;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			limit = Integer.parseInt(conf.get("limit"));
			count = 0;
		}
		
		@Override
		protected void reduce(Text key, Iterable<Text> arg1,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			if (count < limit) {
				for(Text values:arg1){
					context.write(key,values);
				}
				count++;
			}

		}
		
	}
	private Configuration conf;
	public Configuration getConf() {
		return conf;
	}

	public void setConf(Configuration conf) {
		this.conf=conf;
	}

	public int run(String[] args) throws Exception {
		// initializing the job configuration
		Job wordCountJob = new Job(getConf());

		// setting the job name
		wordCountJob.setJobName("Iwinner IT TOP10 Descing Order");

		// to call this as a jar
		wordCountJob.setJarByClass(this.getClass());
	
			
		// setting custom mapper class
		wordCountJob.setMapperClass(CountryByCountDescOrderMapper.class);
    	
		// setting custom reducer class
	   wordCountJob.setReducerClass(CountryByCountDescOrderReducer.class);

		wordCountJob.setSortComparatorClass(SortComparator.class);
		
		wordCountJob.setGroupingComparatorClass(ValueComparator.class);		
		
		// setting custom combinder class
		// wordCountJob.setCombinerClass(WordCountReducer.class);

		// setting no of reducers
		// wordCountJob.setNumReduceTasks(0);

		// setting custom partitioner class
		// wordCountJob.setPartitionerClass(WordCountPartitioner.class);

		// setting mapper output key class: K2		
		wordCountJob.setMapOutputKeyClass(Text.class);
		// setting mapper output value class: V2
		wordCountJob.setMapOutputValueClass(Text.class);
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
	
	

}
