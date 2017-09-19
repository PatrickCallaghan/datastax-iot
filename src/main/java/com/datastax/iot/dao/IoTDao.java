package com.datastax.iot.dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.demo.utils.Timer;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.timeseries.model.DeviceDataPoint;
import com.datastax.timeseries.model.DeviceStat;
import com.datastax.timeseries.model.TimeSeries;
import com.datastax.timeseries.utils.DateUtils;
import com.datastax.timeseries.utils.TimeSeriesUtils;

public class IoTDao {

	private static Logger logger = LoggerFactory.getLogger(IoTDao.class);
	private Session session;

	private static String keyspaceName = "datastax";
	private static String deviceStatsTable = keyspaceName + ".device_stats";
	private static String deviceDataTable = keyspaceName + ".device_data_bucket";
	private static String deviceCompressedTable = keyspaceName + ".device_data_compressed";
	
	private static String INSERT_DATA = "insert into " + deviceDataTable + " (id, year_month_day, time, value) values (?,?,?,?)";
	private static String INSERT_COMPRESSED = "insert into " + deviceCompressedTable + " (id, year_month_day, values) values (?,?,?)";
	private static String INSERT_STATS = "insert into " + deviceStatsTable + " (id, year_month_day, stat_name, stat_value) values (?,?,?,?)";
		
	private List<KeyspaceMetadata> keyspaces;
	private PreparedStatement insertData;
	private PreparedStatement insertStats;
	private PreparedStatement insertCompressed;
	
	private ObjectMapper objectMapper = new ObjectMapper();
	
	public IoTDao(String[] contactPoints) {

		Cluster cluster = Cluster.builder().addContactPoints(contactPoints).build();

		this.session = cluster.connect();
		this.keyspaces = cluster.getMetadata().getKeyspaces();
		
		this.insertData = session.prepare(INSERT_DATA);
		this.insertCompressed = session.prepare(INSERT_COMPRESSED);
		this.insertStats = session.prepare(INSERT_STATS);
	}
	
	public void insertDeviceData(DeviceDataPoint dataPoint){
		ResultSet result = this.session.execute(insertData.bind(dataPoint.getDeviceId(), dataPoint.getYearMonthDay(), dataPoint.getTime(), dataPoint.getValue()));
	}
	
	public void insertDeviceCompressed(TimeSeries timeSeries) throws JsonGenerationException, JsonMappingException, IOException{
		ResultSet result = this.session.execute(insertCompressed.bind(timeSeries.getSymbol(), timeSeries.getYearMonthDay(), objectMapper.writeValueAsString(timeSeries)));		
	}
	
	public void insertDeviceStats(DeviceStat deviceStat){
		ResultSet result = this.session.execute(insertStats.bind());		
	}
	
	
	public TimeSeries getTimeSeries(String exchange, String symbol, DateTime fromDate, DateTime toDate) {
		TimeSeries result = null;
		
		DateTime endOfMonth = fromDate;
		
		Timer timer = new Timer();
		while (endOfMonth.isBefore(toDate)){
			
			if (endOfMonth.getMonthOfYear() == 12){
				endOfMonth = endOfMonth.plusYears(1);
			}
			endOfMonth = endOfMonth.withTimeAtStartOfDay().withMonthOfYear(endOfMonth.plusMonths(1).getMonthOfYear()).withDayOfMonth(1);
			
			logger.info(endOfMonth.toDate().toString());
			TimeSeries timeSeries;
			if (toDate.isBefore(endOfMonth)){
				timeSeries = getTimeSeriesByMonth(symbol, fromDate, toDate);
			}else{
				timeSeries = getTimeSeriesByMonth(symbol, fromDate, endOfMonth);
				fromDate = endOfMonth;
			}
			
			result = TimeSeriesUtils.mergeTimeSeries(result, timeSeries);
			result = TimeSeriesUtils.filter(result, fromDate.getMillis(), toDate.getMillis());
		}
		timer.end();
		logger.info("Request took " + timer.getTimeTakenMillis() + " over a total of " + result.getDates().length + " ticks");
		
		return result;
	}
	
	public TimeSeries getTimeSeriesByMonth(String symbol, DateTime fromDate, DateTime toDate) {		
		List<DateTime> dates = DateUtils.getDatesBetween(fromDate, toDate);
		
		logger.info("Getting timeseries for " + fromDate.toDate() + " to " + toDate.toDate());
		
		Timer t = new Timer();
		List<TimeSeries> timeSeriesDays = getTimeSeriesForDates(symbol, dates, toDate);
		
		TimeSeries finalTimeSeries = null;
				
		for (TimeSeries timeSeries : timeSeriesDays){
			finalTimeSeries =  TimeSeriesUtils.mergeTimeSeries(finalTimeSeries, timeSeries);					
		}
		t.end();
		logger.info("Get " + symbol + " took " + t.getTimeTakenMillis() + "ms " + finalTimeSeries.getDates().length + " points.");
		return finalTimeSeries;
	}

	private List<TimeSeries> getTimeSeriesForDates(String symbol, List<DateTime> dates, DateTime toDate) {
		List<Future<TimeSeries>> results = new ArrayList<Future<TimeSeries>>();
		List<TimeSeries> timeSeriesDays = new ArrayList<TimeSeries>();
		
		DateTime todayMidnight = new DateTime().withMillisOfDay(0);
		
		for (DateTime dateTime : dates){
			
			if (dateTime.isBefore(todayMidnight)){			
				//results.add(new TimeSeriesBinaryReader(tickDataBinaryDao, key));
			}else{
				//results.add(new TimeSeriesReader(tickDataDao, exchange, symbol, todayMidnight, toDate));
			}
		}
		
		for (Future<TimeSeries> future : results) {
			try {
				TimeSeries timeSeries = future.get();
			
				timeSeriesDays.add(timeSeries);
			} catch (InterruptedException | ExecutionException e) {				
				e.printStackTrace();
			}
		}
		return timeSeriesDays;
	}
	public List<KeyspaceMetadata> getKeyspaces() {
		return keyspaces;
	}

}
