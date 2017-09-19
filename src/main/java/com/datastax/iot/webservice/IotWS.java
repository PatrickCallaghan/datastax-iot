package com.datastax.iot.webservice;

import java.text.SimpleDateFormat;
import java.util.List;

import javax.jws.WebService;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.iot.service.IoTService;

@WebService
@Path("/")
public class IoTWS {

	private Logger logger = LoggerFactory.getLogger(IoTWS.class);
	private SimpleDateFormat inputDateFormat = new SimpleDateFormat("yyyyMMdd");

	//Service Layer.
	private IoTService service = IoTService.getInstance();
	
	@GET
	@Path("/get/keyspaces")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getMovements() {
				
		List<KeyspaceMetadata> keyspaces = service.getKeyspaces();		
		return Response.status(Status.OK).entity(keyspaces.toString()).build();
	}
}
