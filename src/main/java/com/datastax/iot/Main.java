package com.datastax.iot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.demo.utils.Timer;
import com.datastax.iot.service.IoTService;

public class Main {

	private static Logger logger = LoggerFactory.getLogger(Main.class);

	public Main() {

		IoTService service = IoTService.getInstance();		
		Timer timer = new Timer();
		
		//Do something here.
		logger.info(service.getKeyspaces().toString());
		
		timer.end();
		logger.info("Test took " +timer.getTimeTakenSeconds() + " secs.");
		System.exit(0);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new Main();

		System.exit(0);
	}

}
