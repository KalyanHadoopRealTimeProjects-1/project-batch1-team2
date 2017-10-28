package com.eventproces.mapreduce.usecase4.dbxml;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class EventProcessWritable implements WritableComparable<EventProcessWritable>, DBWritable {

	// 2013-11-24T19:08:21|189.89.146.110|FR|SUCCESS

	public static String[] fields = { "processdate", "ip", "country", "status" };

	private String processdate;
	private String ip;
	private String country;
	private String status;
	

	public String getProcessdate() {
		return processdate;
	}

	public void setProcessdate(String processdate) {
		this.processdate = processdate;
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
		out.writeUTF(processdate);
		out.writeUTF(ip);
		out.writeUTF(country);
		out.writeUTF(status);
	}

	
	public void readFields(DataInput in) throws IOException {
		processdate = in.readUTF();
		ip = in.readUTF();
		country = in.readUTF();
		status = in.readUTF();
	}

	public int compareTo(EventProcessWritable key) {
		return CompareToBuilder.reflectionCompare(this, key);
	}

	public void write(PreparedStatement statement) throws SQLException {
		int idx = 1;
		statement.setString(idx++, getProcessdate());
		statement.setString(idx++, getIp());
		statement.setString(idx++, getCountry());
		statement.setString(idx++, getStatus());
	}

	public void readFields(ResultSet resultSet) throws SQLException {

		int idx = 1;
		setProcessdate(resultSet.getString(idx++));
		setIp(resultSet.getString(idx++));
		setCountry(resultSet.getString(idx++));
		setStatus(resultSet.getString(idx++));
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((country == null) ? 0 : country.hashCode());
		result = prime * result + ((ip == null) ? 0 : ip.hashCode());
		result = prime * result + ((processdate == null) ? 0 : processdate.hashCode());
		result = prime * result + ((status == null) ? 0 : status.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		EventProcessWritable other = (EventProcessWritable) obj;
		if (country == null) {
			if (other.country != null)
				return false;
		} else if (!country.equals(other.country))
			return false;
		if (ip == null) {
			if (other.ip != null)
				return false;
		} else if (!ip.equals(other.ip))
			return false;
		if (processdate == null) {
			if (other.processdate != null)
				return false;
		} else if (!processdate.equals(other.processdate))
			return false;
		if (status == null) {
			if (other.status != null)
				return false;
		} else if (!status.equals(other.status))
			return false;
		return true;
	}

	public String toString() {
		return "EventProcessWritable [processdate=" + processdate + ", ip=" + ip + ", country=" + country + ", status="
				+ status + "]";
	}

	
	

}
