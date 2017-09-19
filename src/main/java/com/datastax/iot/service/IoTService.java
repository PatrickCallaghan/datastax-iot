package com.datastax.iot.service;

import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.demo.utils.PropertyHelper;
import com.datastax.demo.utils.Timer;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.iot.dao.IoTDao;
import com.datastax.iot.dao.TimeSeriesJsonReader;
import com.datastax.iot.dao.TimeSeriesReader;
import com.datastax.timeseries.model.TimeSeries;
import com.datastax.timeseries.utils.DateUtils;
import com.datastax.timeseries.utils.TimeSeriesUtils;
import com.google.common.util.concurrent.ListenableFuture;

public class IoTService {
	private static Logger logger = LoggerFactory.getLogger(IoTService.class);
	private static final Format formatter = new SimpleDateFormat("yyyyMMdd");
	
	private IoTDao dao;	
	private static final IoTService service = new IoTService();

	private IoTService() {		
		String contactPointsStr = PropertyHelper.getProperty("contactPoints", "localhost");
		this.dao = new IoTDao(contactPointsStr.split(","));
	}	
	
	public List<KeyspaceMetadata> getKeyspaces() {
		return dao.getKeyspaces();
	}

	public static IoTService getInstance() {
		return service;
	}

	public void convertRawToCompressed(String device, DateTime dateTime) {
		
		int yearMonthDay = Integer.parseInt(formatter.format(dateTime.toDate()));
		
		try {
			dao.getTimeSeries(device, yearMonthDay);
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
	}
	
	public TimeSeries getDeviceData(String deviceId, DateTime from, DateTime to){
				
		return getTimeSeries(deviceId, from, to);
	}
	
	
	public TimeSeries getTimeSeries(String symbol, DateTime fromDate, DateTime toDate) {
		TimeSeries result = null;

		DateTime endOfMonth = fromDate;

		Timer timer = new Timer();
		while (endOfMonth.isBefore(toDate)) {

			if (endOfMonth.getMonthOfYear() == 12) {
				endOfMonth = endOfMonth.plusYears(1);
			}
			endOfMonth = endOfMonth.withTimeAtStartOfDay().withMonthOfYear(endOfMonth.plusMonths(1).getMonthOfYear())
					.withDayOfMonth(1);

			logger.info(endOfMonth.toDate().toString());
			TimeSeries timeSeries;
			if (toDate.isBefore(endOfMonth)) {
				timeSeries = getTimeSeriesByMonth(symbol, fromDate, toDate);
			} else {
				timeSeries = getTimeSeriesByMonth(symbol, fromDate, endOfMonth);
				fromDate = endOfMonth;
			}

			result = TimeSeriesUtils.mergeTimeSeries(result, timeSeries);
		}
		result = TimeSeriesUtils.filter(result, fromDate.getMillis(), toDate.getMillis());

		timer.end();
		logger.info("Request took " + timer.getTimeTakenMillis() + " over a total of " + result.getDates().length
				+ " data points");

		return result;
	}

	public TimeSeries getTimeSeriesByMonth(String symbol, DateTime fromDate, DateTime toDate) {
		List<DateTime> dates = DateUtils.getDatesBetween(fromDate, toDate);

		logger.info("Getting timeseries for " + fromDate.toDate() + " to " + toDate.toDate());

		Timer t = new Timer();
		List<TimeSeries> timeSeriesDays = getTimeSeriesForDates(symbol, dates, toDate);

		TimeSeries finalTimeSeries = null;

		for (TimeSeries timeSeries : timeSeriesDays) {
			finalTimeSeries = TimeSeriesUtils.mergeTimeSeries(finalTimeSeries, timeSeries);
		}
		t.end();
		logger.info("Get " + symbol + " took " + t.getTimeTakenMillis() + "ms " + finalTimeSeries.getDates().length
				+ " points.");
		return finalTimeSeries;
	}

	private List<TimeSeries> getTimeSeriesForDates(String symbol, List<DateTime> dates, DateTime toDate) {
		List<ListenableFuture<TimeSeries>> results = new ArrayList<ListenableFuture<TimeSeries>>();
		List<TimeSeries> timeSeriesDays = new ArrayList<TimeSeries>();

		DateTime tMinus1Midnight = new DateTime().minusDays(1).withMillisOfDay(0);
		for (DateTime dateTime : dates) {

			if (dateTime.isBefore(tMinus1Midnight)) {
				results.add(new TimeSeriesJsonReader(dao, symbol, getYearMonthDay(dateTime)));
			} else {
				results.add(new TimeSeriesReader(dao, symbol, getYearMonthDay(dateTime)));
			}
		}

		for (ListenableFuture<TimeSeries> future : results) {
			try {
				TimeSeries timeSeries = future.get();
				timeSeriesDays.add(timeSeries);
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
		}
		return timeSeriesDays;
	}

	private int getYearMonthDay(DateTime dateTime) {
		return Integer.parseInt(formatter.format(dateTime.toDate()));
	}

}
