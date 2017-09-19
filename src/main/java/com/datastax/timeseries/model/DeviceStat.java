package com.datastax.timeseries.model;

public class DeviceStat {

	private String deviceId;
	private int yearMonthDay;
	private String statName;
	private double statValue;
	
	public String getDeviceId() {
		return deviceId;
	}
	public void setDeviceId(String deviceId) {
		this.deviceId = deviceId;
	}
	public int getYearMonthDay() {
		return yearMonthDay;
	}
	public void setYearMonthDay(int yearMonthDay) {
		this.yearMonthDay = yearMonthDay;
	}
	public String getStatName() {
		return statName;
	}
	public void setStatName(String statName) {
		this.statName = statName;
	}
	public double getStatValue() {
		return statValue;
	}
	public void setStatValue(double statValue) {
		this.statValue = statValue;
	}
	@Override
	public String toString() {
		return "DeviceStat [deviceId=" + deviceId + ", yearMonthDay=" + yearMonthDay + ", statName=" + statName
				+ ", statValue=" + statValue + "]";
	}
	
	
}
