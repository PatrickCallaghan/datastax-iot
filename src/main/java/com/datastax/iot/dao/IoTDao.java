package com.datastax.iot.dao;

import java.io.IOException;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

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
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.timeseries.model.DeviceDataPoint;
import com.datastax.timeseries.model.DeviceStat;
import com.datastax.timeseries.model.TimeSeries;
import com.datastax.timeseries.utils.DateUtils;
import com.datastax.timeseries.utils.TimeSeriesUtils;
import com.google.common.util.concurrent.ListenableFuture;

import cern.colt.list.DoubleArrayList;
import cern.colt.list.LongArrayList;

public class IoTDao {

	private static Logger logger = LoggerFactory.getLogger(IoTDao.class);
	private static final Format formatter = new SimpleDateFormat("yyyyMMdd");
	private Session session;

	private static String keyspaceName = "datastax";
	private static String deviceStatsTable = keyspaceName + ".device_stats";
	private static String deviceDataTable = keyspaceName + ".device_data_bucket";
	private static String deviceCompressedTable = keyspaceName + ".device_data_compressed";

	private static String INSERT_DATA = "insert into " + deviceDataTable
			+ " (id, year_month_day, time, value) values (?,?,?,?)";
	private static String INSERT_COMPRESSED = "insert into " + deviceCompressedTable
			+ " (id, year_month_day, values) values (?,?,?)";
	private static String INSERT_STATS = "insert into " + deviceStatsTable
			+ " (id, year_month_day, stat_name, stat_value) values (?,?,?,?)";

	private static final String SELECT_FROM_DATA = "Select * from " + deviceDataTable + " where device = ? and year_month_day =?";
	
	private List<KeyspaceMetadata> keyspaces;
	private PreparedStatement insertData;
	private PreparedStatement insertStats;
	private PreparedStatement insertCompressed;
	private PreparedStatement selectData;

	private ObjectMapper objectMapper = new ObjectMapper();

	public IoTDao(String[] contactPoints) {

		Cluster cluster = Cluster.builder().addContactPoints(contactPoints).build();

		this.session = cluster.connect();
		this.keyspaces = cluster.getMetadata().getKeyspaces();

		this.insertData = session.prepare(INSERT_DATA);
		this.insertCompressed = session.prepare(INSERT_COMPRESSED);
		this.insertStats = session.prepare(INSERT_STATS);
		this.selectData = session.prepare(SELECT_FROM_DATA);
	}

	public void insertDeviceData(DeviceDataPoint dataPoint) {
		this.session.execute(insertData.bind(dataPoint.getDeviceId(), dataPoint.getYearMonthDay(), dataPoint.getTime(),
				dataPoint.getValue()));
	}

	public void insertDeviceCompressed(TimeSeries timeSeries)
			throws JsonGenerationException, JsonMappingException, IOException {
		this.session.execute(insertCompressed.bind(timeSeries.getSymbol(), timeSeries.getYearMonthDay(),
				objectMapper.writeValueAsString(timeSeries)));
	}

	public void insertDeviceStats(DeviceStat deviceStat) {
		this.session.execute(insertStats.bind());
	}

	/**
	 * Get data from a time bucket
	 * @param device
	 * @param yearMonthDay
	 * @return
	 */
	
	public ResultSetFuture getTimeSeriesDataFuture(String device, int yearMonthDay){
		return session.executeAsync(this.selectData.bind(device, yearMonthDay));
	}

	public TimeSeries getTimeSeries(String device, int yearMonthDay) throws InterruptedException, ExecutionException {

		ResultSetFuture future = this.getTimeSeriesCompressedFuture(device, yearMonthDay);	
		Iterator<Row> iterator = future.get().iterator();
		
		DoubleArrayList values = new DoubleArrayList();
		LongArrayList dates = new LongArrayList();

		while (iterator.hasNext()) {
			Row row = iterator.next();

			dates.add(row.getTimestamp("date").getTime());
			values.add(row.getDouble("value"));
		}

		dates.trimToSize();
		values.trimToSize();
		
		return new TimeSeries(device, dates.elements(), values.elements());
	}
	
	public ResultSetFuture getTimeSeriesCompressedFuture(String device, int yearMonthDay){
		return session.executeAsync(this.selectData.bind(device, yearMonthDay));
	}
	
	public TimeSeries getTimeSeriesCompressed(String device, int yearMonthDay) throws InterruptedException, ExecutionException {

		ResultSetFuture future = this.getTimeSeriesCompressedFuture(device, yearMonthDay);
		Iterator<Row> iterator = future.get().iterator();
		
		DoubleArrayList values = new DoubleArrayList();
		LongArrayList dates = new LongArrayList();

		while (iterator.hasNext()) {
			Row row = iterator.next();

			dates.add(row.getTimestamp("date").getTime());
			values.add(row.getDouble("value"));
		}

		dates.trimToSize();
		values.trimToSize();
		
		return new TimeSeries(device, dates.elements(), values.elements());
	}
	
	/**
	 * Get data over a bigger time frame
	 * @param symbol
	 * @param fromDate
	 * @param toDate
	 * @return
	 */

	public List<KeyspaceMetadata> getKeyspaces() {
		return keyspaces;
	}

}
