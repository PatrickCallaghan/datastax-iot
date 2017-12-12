package com.datastax.iot;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Test;

import com.datastax.iot.service.IoTService;

public class TestService {

	private DateTimeFormatter parser = DateTimeFormat.forPattern("yyyyMMddHHmmss");
	
	@Test
	public void testService(){
		IoTService service = IoTService.getInstance();

		DateTime fromDate = parser.parseDateTime("20170825000000");
		DateTime toDate = parser.parseDateTime("20170907235959");

		
		service.getTimeSeries("D-1", fromDate, toDate);
	}
}
