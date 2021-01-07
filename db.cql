CREATE KEYSPACE IF NOT EXISTS data_center WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 2};
use data_center;

CREATE TABLE if not exists ts(
    name varchar,
    data_record varchar,
    PRIMARY KEY(name));

CREATE TABLE if not exists ts_data(
  type varchar,
  visible_time timestamp,
  values varchar,
  code  varchar,
  PRIMARY KEY((type, code), visible_time));

create type order(
    type varchar,
    code varchar,
    direction varchar,
    quantity float,
    place_time timestamp,
    filled_start_time timestamp,
    filled_end_time timestamp,
    status varchar,
    filled_quantity float,
    filled_avg_price float,
    fee float,
    delay_time timestamp,
    limit_price float,
    cross_price float,
    cross_direction varchar
    );

create type operation(
    start_time timestamp,
    end_time timestamp,
    pnl float,
    orders list<order>
    );

create table if not exists account(
    name varchar,
    cash float,
    initial_cash float,
    positions map<varchar, float>,
    history_net_value map<timestamp, float>,
    current_operation operation,
    history_operations list<operation>,
    primary key (name)
)