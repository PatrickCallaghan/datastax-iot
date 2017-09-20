package com.datastax.iot;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.demo.utils.PropertyHelper;
import com.datastax.iot.service.IoTService;

/**
 * The main class to run the conversion service. This will IoT service to create statistics and compress daily data. 	
 * @author patrickcallaghan
 *
 */
public class DailyConversionService {
	private static Logger logger = LoggerFactory.getLogger(DailyConversionService.class);
	private IoTService service;

	public DailyConversionService() {
		this.service = IoTService.getInstance();
	}

	public void runCompressedConversionForDevice(String device, DateTime dateTime) {

		service.convertRawToCompressed(device, dateTime);
	}

	public static void main(String args[]) {
		DailyConversionService service = new DailyConversionService();
		service.start();
	}

	private void start() {
		int noOfDays = Integer.parseInt(PropertyHelper.getProperty("noOfDays", "30"));
		int devicesStart = Integer.parseInt(PropertyHelper.getProperty("firstDevice", "1"));
		int noOfDevices = Integer.parseInt(PropertyHelper.getProperty("noOfDevices", "10"));

		DateTime startDate = DateTime.now().withMillisOfDay(0).minusDays(noOfDays);

		while (startDate.isBefore(DateTime.now().minusDays(1).withMillisOfDay(0))) {

			logger.info("Processing " + startDate);
			for (int i = devicesStart; i < (devicesStart + noOfDevices); i++) {
				int deviceId = i;

				logger.info("Processing device : D-" + deviceId + " for " + startDate);
				runCompressedConversionForDevice("D-" + deviceId, startDate);
			}
			startDate = startDate.plusDays(1);
		}

		sleep(1000);
		System.exit(0);	
	}

	private static void sleep(int i) {
		try {
			Thread.sleep(i);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
