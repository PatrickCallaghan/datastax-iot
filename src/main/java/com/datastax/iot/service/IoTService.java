package com.datastax.iot.service;

import java.io.IOException;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.demo.utils.PropertyHelper;
import com.datastax.demo.utils.Timer;
import com.datastax.iot.dao.IoTDao;
import com.datastax.iot.dao.TimeSeriesJsonReader;
import com.datastax.iot.dao.TimeSeriesReader;
import com.datastax.timeseries.model.DeviceDataPoint;
import com.datastax.timeseries.model.TimeSeries;
import com.datastax.timeseries.utils.DateUtils;
import com.datastax.timeseries.utils.TimeSeriesHelper;
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
	
	public static IoTService getInstance() {
		return service;
	}

	/**
	 * For a device and a specific data, get the raw data, create statistic and write compressed version
	 * @param device device id 
	 * @param dateTime day to be processed. 
	 */
	public void convertRawToCompressed(String device, DateTime dateTime) {
		
		int yearMonthDay = Integer.parseInt(formatter.format(dateTime.toDate()));
		
		try {
			TimeSeries timeSeries = dao.getTimeSeries(device, yearMonthDay);
			dao.insertDeviceCompressed(timeSeries);
			
			dao.insertDeviceStatsList(TimeSeriesUtils.stats(timeSeries));
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		} catch (JsonGenerationException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public TimeSeries getDeviceData(String deviceId, DateTime from, DateTime to){
				
		return getTimeSeries(deviceId, from, to);
	}	
	
	/**
	 * Internal processing of getting TimeSeries data
	 * @param symbol
	 * @param fromDate
	 * @param toDate
	 * @return
	 */
	public TimeSeries getTimeSeries(String symbol, DateTime fromDate, DateTime toDate) {
		TimeSeries result = null;
		DateTime endOfMonth = fromDate;
		
		//For filtering
		DateTime startFilterDate = fromDate;
		DateTime endFilterDate = toDate;

		Timer timer = new Timer();
		
		//Process will get the data in parrallel by month to avoid overloading the cluster. 
		//This could be improved with a larger cluster. 
		while (endOfMonth.isBefore(toDate)) {

			if (endOfMonth.getMonthOfYear() == 12) {
				endOfMonth = endOfMonth.plusYears(1);
			}
			endOfMonth = endOfMonth.withTimeAtStartOfDay().withMonthOfYear(endOfMonth.plusMonths(1).getMonthOfYear())
					.withDayOfMonth(1);

			TimeSeries timeSeries;
			
			if (toDate.isBefore(endOfMonth)) {
				timeSeries = getTimeSeriesByMonth(symbol, fromDate, toDate);
			} else {
				timeSeries = getTimeSeriesByMonth(symbol, fromDate, endOfMonth);
				fromDate = endOfMonth;
			}

			//Merge the results 
			result = TimeSeriesUtils.mergeTimeSeries(result, timeSeries);			
		}
		result = TimeSeriesUtils.filter(result, startFilterDate.getMillis(), endFilterDate.getMillis());

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
		logger.info("Data for " + symbol + " took " + t.getTimeTakenMillis() + "ms " + finalTimeSeries.getDates().length
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

	public void save(DeviceDataPoint deviceDataPoint) {
		dao.insertDeviceData(deviceDataPoint);		
	}

}
