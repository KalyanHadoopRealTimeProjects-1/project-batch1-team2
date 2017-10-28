package com.eventproces.mapreduce.usecase2.top10;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * 
 * 
 * Custom EventProcessLog DataType for Processing data
 * @author hadoop
 *
 */
public class EventProcessLog implements Writable {

	private String processtimestamp;
	private String ip;
	private String country;
	private String status;

	public String getProcesstimestamp() {
		return processtimestamp;
	}

	public void setProcesstimestamp(String processtimestamp) {
		this.processtimestamp = processtimestamp;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public void write(DataOutput out) throws IOException {

		out.writeUTF(processtimestamp);
		out.writeUTF(ip);
		out.writeUTF(country);
		out.writeUTF(status);

	}

	public void readFields(DataInput in) throws IOException {

		processtimestamp = in.readUTF();
		ip = in.readUTF();
		country = in.readUTF();
		status = in.readUTF();
	}

	public String toString() {
		return processtimestamp + "|" + ip + "|" + country + "|" + status;
	}

}
