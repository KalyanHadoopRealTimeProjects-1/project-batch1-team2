package com.eventproces.mapreduce.usecase1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class MainDriverJob {

	
	public static void main(String[] args) {
		
		
		try{
			Configuration conf = new Configuration();
			conf.set("limit", "20");

			String inputPath=args[0];
			String outputPath=args[1];
			
			
		int textStatus = ToolRunner.run(conf,	new TextFileToTextDriverJob(), new String[]{inputPath,outputPath});
		System.out.println("textStatus Code=>"+textStatus);
		
		
		int jsonCode=ToolRunner.run(conf,new JSONFileToJSONDriverJob(), new String[]{inputPath,outputPath});
		System.out.println("jsonCode=>"+jsonCode);
		
		
		int pdfStatusCode = ToolRunner.run(conf,	new PDFToPDFDriverJob(), new String[]{inputPath,outputPath});
		System.out.println("pdfStatusCode Code=>"+pdfStatusCode);
		
		
		int xmlStatusCode=ToolRunner.run(conf,new XmlFileToXmlDriverJob(), new String[]{inputPath,outputPath});
		System.out.println("xmlStatusCode  Code =>"+xmlStatusCode);
		
		}catch(Exception e){
				
		}
	}
}
	