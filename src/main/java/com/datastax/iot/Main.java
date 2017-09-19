package com.datastax.iot;

import java.util.HashMap;
import java.util.Map;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.demo.utils.PropertyHelper;
import com.datastax.demo.utils.Timer;
import com.datastax.iot.service.IoTService;
import com.datastax.timeseries.model.DeviceDataPoint;

public class Main {

	private static Logger logger = LoggerFactory.getLogger(Main.class);

	public Main() {
		int noOfDays = Integer.parseInt(PropertyHelper.getProperty("noOfDays", "30"));
		int devicesStart = Integer.parseInt(PropertyHelper.getProperty("firstDevice", "1"));
		int noOfDevices = Integer.parseInt(PropertyHelper.getProperty("noOfDevices", "10"));

		IoTService service = IoTService.getInstance();		
		Timer timer = new Timer();
		//Start processing data points in chronological order to ensure old ones get pushed out.
		
		DateTime startDate = DateTime.now().withMillisOfDay(0).minusDays(noOfDays);		
		Map<Integer, Double> deviceValues = new HashMap<Integer, Double>(); 
		
		while (startDate.isBefore(DateTime.now().withMillisOfDay(0))){
		
			logger.info("Processing " + startDate);
			for (int i=devicesStart; i < (devicesStart+noOfDevices); i++){				
				int deviceId = i;
				double value;
				
				if (deviceValues.containsKey(deviceId)){
					value = deviceValues.get(deviceId);
					
					if (Math.random() > .5d){
						value = value + Math.random();
					}else{
						value = value - Math.random();
					}
					deviceValues.put(deviceId, value);
				}else{
					value = new Double((Math.random()*10) + 15);
					deviceValues.put(deviceId, value);
				}
				
				service.save(new DeviceDataPoint("D-" + deviceId, startDate.toDate(), value));				
			}
			
			startDate = startDate.plusMinutes(1);
		}
				
		timer.end();
		logger.info("Test took " +timer.getTimeTakenSeconds() + " secs.");
		sleep(1000);
		System.exit(0);
	}

	private void sleep(int i) {
		try {
			Thread.sleep(i);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}		
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new Main();

		System.exit(0);
	}
}