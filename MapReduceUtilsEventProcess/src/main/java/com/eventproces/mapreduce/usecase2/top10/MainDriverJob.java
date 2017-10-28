package com.eventproces.mapreduce.usecase2.top10;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class MainDriverJob {

	public static void main(String[] args) {
		try{
			Configuration conf = new Configuration();
			conf.set("limit", "20");
//		
//			String ratings = args[0];
//			String movies = args[1];
//			String limit = args[2];
//			String output = args[3];
			
			String inputPath=args[0];
			String outputPath=args[1];
			String targetPath=args[2];
			String finalOutPath="part-r-00000";
			
			//new String[] { ratings, movieidcount }
		int exitCode = ToolRunner.run(conf,	new CountryByCountDriverJob(), new String[]{inputPath,outputPath});
		System.out.println("Status Code=>"+exitCode);
		
		
		int descOrder=ToolRunner.run(conf,new CountryByCountDescOrderDriverJob(), new String[]{outputPath+finalOutPath,targetPath});
		System.out.println("Status  Code descOrder=>"+descOrder);
		
		
		System.exit(exitCode);
		}catch(Exception e){
				
		}
	}
}
