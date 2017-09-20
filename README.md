# datastax-iot-demo

This is a small demo to show how to insert meter readings for a smart reader. Note : the readings come in through a file with
a no of readings per day. 

## Schema Setup
Note : This will drop the keyspace "datastax_iot_demo" and create a new one. All existing data will be lost. 

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
    
