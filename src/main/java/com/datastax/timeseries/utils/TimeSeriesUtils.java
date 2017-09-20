package com.datastax.timeseries.utils;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.timeseries.model.DeviceStat;
import com.datastax.timeseries.model.TimeSeries;

import cern.colt.list.DoubleArrayList;
import cern.colt.list.LongArrayList;

public class TimeSeriesUtils {

	private static Logger logger = LoggerFactory.getLogger(TimeSeriesUtils.class);
	public static final String MIN = "min";
	public static final String MAX = "max";
	public static final String AVG = "avg";

	public static TimeSeries filter(TimeSeries timeSeries, long from, long to) {

		if (timeSeries == null) {
			return timeSeries;
		}
		
		if (timeSeries.lowestDate() > from && timeSeries.highestDate() < to){
			return timeSeries;
		}

		long[] oldDates = timeSeries.getDates();
		double[] oldValues = timeSeries.getValues();

		LongArrayList dates = new LongArrayList(oldDates.length);
		DoubleArrayList values = new DoubleArrayList(oldDates.length);

		for (int i = 0; i < oldDates.length; i++) {

			long date = oldDates[i];
			double oldValue = oldValues[i];

			if (date >= from && date < to) {
				dates.add(date);
				values.add(oldValue);
			}
		}

		dates.trimToSize();
		values.trimToSize();

		return new TimeSeries(timeSeries.getSymbol(), dates.elements(), values.elements());
	}

	static public TimeSeries mergeTimeSeries(TimeSeries timeSeries1, TimeSeries timeSeries2) {

		
		if (timeSeries1 == null && timeSeries2 == null) {
			return null;
		}
		if (timeSeries1 == null) {
			return timeSeries2;
		}
		if (timeSeries2 == null) {
			return timeSeries1;
		}

		logger.trace("Merging :" + timeSeries1.getYearMonthDay() + " and " + timeSeries2.getYearMonthDay());
		
		logger.trace("Merging :" + new Date(timeSeries1.lowestDate()) +  "/" + new Date(timeSeries1.highestDate()) 
		+ " and " +new Date(timeSeries2.lowestDate()) +  "/" + new Date(timeSeries2.highestDate()));
		
		if (timeSeries1.highestDate() > timeSeries2.highestDate()
				&& timeSeries1.lowestDate() > timeSeries2.highestDate()) {

			long[] dates = ArrayUtils.addAll(timeSeries1.getDates(), timeSeries2.getDates());
			double[] values = ArrayUtils.addAll(timeSeries1.getValues(), timeSeries2.getValues());
			return new TimeSeries(timeSeries1.getSymbol(), dates, values);

		} else if (timeSeries2.highestDate() > timeSeries1.highestDate()
				&& timeSeries2.lowestDate() > timeSeries1.highestDate()) {

			long[] dates = ArrayUtils.addAll(timeSeries2.getDates(), timeSeries1.getDates());
			double[] values = ArrayUtils.addAll(timeSeries2.getValues(), timeSeries1.getValues());
			return new TimeSeries(timeSeries1.getSymbol(), dates, values);
		} else {
			// TODO : Do some fancy sorting
			return timeSeries1;
		}
	}

	static public Long[] concat(long[] a, long[] b) {
		int aLen = a.length;
		int bLen = b.length;
		Long[] c = new Long[aLen + bLen];
		System.arraycopy(a, 0, c, 0, aLen);
		System.arraycopy(b, 0, c, aLen, bLen);
		return c;
	}

	static public Double[] concat(double[] a, double[] b) {
		int aLen = a.length;
		int bLen = b.length;
		Double[] c = new Double[aLen + bLen];
		System.arraycopy(a, 0, c, 0, aLen);
		System.arraycopy(b, 0, c, aLen, bLen);
		return c;
	}

	/**
	 * Creates some sample statistics for a TimeSeries
	 * @param timeSeries
	 * @return
	 */
	static public List<DeviceStat> stats(TimeSeries timeSeries) {

		double[] numbers = timeSeries.getValues();
		double smallest = numbers[0];
		double largest = numbers[0];

		double total = numbers[0]; 
		for (int i = 1; i < numbers.length; i++) {
			if (numbers[i] > largest)
				largest = numbers[i];
			else if (numbers[i] < smallest)
				smallest = numbers[i];

			total = total + numbers[i];
		}

		List<DeviceStat> deviceStats = new ArrayList<DeviceStat>();
		
		deviceStats.add(new DeviceStat(timeSeries.getSymbol(), timeSeries.getYearMonthDay(), MIN, smallest));
		deviceStats.add(new DeviceStat(timeSeries.getSymbol(), timeSeries.getYearMonthDay(), MAX, largest));
		deviceStats.add(new DeviceStat(timeSeries.getSymbol(), timeSeries.getYearMonthDay(), AVG, total/numbers.length));
		
		return deviceStats;
	}
}
