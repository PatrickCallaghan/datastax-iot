CREATE KEYSPACE datastax WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1' };

Create table datastax.device_data (
	id text,
	time timestamp,
	value int,
	PRIMARY KEY (id, time)
) with clustering order by (time desc);

Create table datastax.device_data_bucket (
	id text,
	year_month_day int,
	time timestamp,
	value int,
	PRIMARY KEY ((id, year_month_day), time)
) with clustering order by (time desc); 


Create table datastax.device_data_compressed (
	id text,
	year_month_day int,
	values text,
	PRIMARY KEY ((id, year_month_day))
);

Create table datastax.device_stats (
	id text,
	year_month_day int,
	stat_name text,
	stat_value int,
	PRIMARY KEY (id, year_month_day, stat_name)
) with clustering order by (year_month_day desc, stat_name asc);


