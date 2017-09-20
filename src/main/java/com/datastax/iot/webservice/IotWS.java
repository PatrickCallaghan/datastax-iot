package com.datastax.iot.webservice;

import javax.jws.WebService;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.iot.service.IoTService;

@WebService
@Path("/")
public class IoTWS {

	private Logger logger = LoggerFactory.getLogger(IoTWS.class);
	private DateTimeFormatter parser = DateTimeFormat.forPattern("yyyyMMddHHmmss");

	//Service Layer.
	private IoTService service = IoTService.getInstance();
	
	@GET
	@Path("/get/bydatetime/{device}/{fromdate}/{todate}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getMeasurementSeries(@PathParam("device") String deviceId,
			@PathParam("fromdate") String fromDateString, @PathParam("todate") String toDateString){
			
		DateTime fromDate = parser.parseDateTime(fromDateString);
		DateTime toDate = parser.parseDateTime(toDateString);
					
		return Response.status(201).entity(service.getDeviceData(deviceId, fromDate, toDate)).build();
	}

	@GET
	@Path("/get/bydatetime/")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getTestSeries(){
			
					
		return Response.status(201).entity("Success").build();
	}
	
	
}
