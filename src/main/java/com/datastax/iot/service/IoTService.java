package com.datastax.iot.service;

import java.util.List;

import org.joda.time.DateTime;

import com.datastax.demo.utils.PropertyHelper;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.iot.dao.IoTDao;

public class IoTService {

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
		
	}
}
