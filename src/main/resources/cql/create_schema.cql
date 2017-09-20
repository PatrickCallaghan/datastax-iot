CREATE KEYSPACE if not exists datastax WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1' };

Create table  if not exists datastax.device_data (
	id text,
	time timestamp,
	value double,
	PRIMARY KEY (id, time)
) with clustering order by (time desc);

Create table  if not exists datastax.device_data_bucket (
	id text,
	year_month_day int,
	time timestamp,
	value double,
	PRIMARY KEY ((id, year_month_day), time)
) with clustering order by (time desc); 


Create table  if not exists datastax.device_data_compressed (
	id text,
	year_month_day int,
	values text,
	PRIMARY KEY ((id, year_month_day))
);

Create table  if not exists datastax.device_stats (
	id text,
	year_month_day int,
	stat_name text,
	stat_value double,
	PRIMARY KEY (id, year_month_day, stat_name)
) with clustering order by (year_month_day desc, stat_name asc);


