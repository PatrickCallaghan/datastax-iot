# DataStax IoT

This is a small demo to show how to readings for a smart device. The readings are realtime and come are every 10 secs for a device.  

## Schema Setup
To specify contact points use the contactPoints command line parameter e.g. 

	'-DcontactPoints=192.168.25.100,192.168.25.101'
	
The contact points can take mulitple points in the IP,IP,IP (no spaces).

To create the a single node cluster with replication factor of 1 for standard localhost setup, run the following

To create the schema, run the following

	mvn clean compile exec:java -Dexec.mainClass="com.datastax.demo.SchemaSetup" -DcontactPoints=localhost
		
To insert some readings, run the following 
	
	mvn clean compile exec:java -Dexec.mainClass="com.datastax.iot.Main" -DcontactPoints=localhost
	
To run the statistics and compressor process 

	mvn clean compile exec:java -Dexec.mainClass="com.datastax.iot.DailyConversionService" -DcontactPoints=localhost

To remove the tables and the schema, run the following.

    mvn clean compile exec:java -Dexec.mainClass="com.datastax.demo.SchemaTeardown" -DcontactPoints=localhost
    

