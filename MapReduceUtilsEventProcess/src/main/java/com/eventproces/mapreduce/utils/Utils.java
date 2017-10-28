package com.eventproces.mapreduce.utils;

public class Utils {

	public static void main(String[] args) {
		
		
		String str="2013-11-24T19:08:21|133.134.238.168|FR|SUCCESS";
		
		String sp[]=str.split("\\|");
		System.out.println(sp[2]);
	}
}
