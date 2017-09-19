package com.datastax.timeseries.model;

import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DeviceDataPoint {

	private static final Format formatter = new SimpleDateFormat("yyyyMMdd");
	
	private String deviceId;
	private Date time;
	private double value;
	
	public DeviceDataPoint(String deviceId, Date time, double value) {
		super();
		this.deviceId = deviceId;
		this.time = time;
		this.value = value;
	}
	public String getDeviceId() {
		return deviceId;
	}
	public void setDeviceId(String deviceId) {
		this.deviceId = deviceId;
	}
	public Date getTime() {
		return time;
	}
	public void setTime(Date time) {
		this.time = time;
	}
	public double getValue() {
		return value;
	}
	public void setValue(double value) {
		this.value = value;
	}
	public int getYearMonthDay() {
		return Integer.parseInt(formatter.format(getTime()));
	}
	@Override
	public String toString() {
		return "DeviceDataPoint [deviceId=" + deviceId + ", time=" + time + ", value=" + value + "]";
	}

}
