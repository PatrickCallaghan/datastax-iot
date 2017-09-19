package com.datastax.iot;

import org.joda.time.DateTime;

import com.datastax.iot.service.IoTService;

public class DailyConversionService {
	
	private IoTService service;
	
	public DailyConversionService() {
		this.service = IoTService.getInstance();
	}

	/**
	 * Convert all exchange symbols to binary for today. 
	 */
	public void runTickDataToBinaryConversion(DateTime dateTime){
		
	}

	public void runTickDataToBinaryConversionForSymbol(String device, DateTime dateTime){
					
		service.convertRawToCompressed(device, dateTime);					
	}

	
	public static void main(String args[]){
		DailyConversionService service = new DailyConversionService();

		service.runTickDataToBinaryConversion(DateTime.now());
		
		System.exit(0);
	}
	
}
