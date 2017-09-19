create keyspace if not exists sample_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1' };

use sample_keyspace; 

create table if not exists test (a text PRIMARY KEY, b text);


