use default;

drop table if exists test_data_types;
create table test_data_types (
   id int,
   tinyint_field tinyint,
   smallint_field smallint,
   int_field int,
   bigint_field bigint,
   bool_field boolean,
   float_field float,
   double_field double,
   decimal_field decimal,
   string_field string,
   char_field char(40),
   varchar_field varchar(100),
   date_field date,
   timestamp_field timestamp
);

insert into test_data_types(id, tinyint_field, smallint_field, int_field, bigint_field, bool_field, float_field, double_field, decimal_field, string_field, char_field, varchar_field, date_field, timestamp_field)
values(1, 1, 1, 1, 1, true, 0.55, 0.123, 3.14, 'test', 'test', 'test', '2019-04-09', '2019-04-09 08:00:00');

drop table if exists test_field_names;
create table test_field_names (
   id int,
   low_case int,
   UPCASE int,
   CamelCase int,
   `Table` int,
   `array` int,
   `SELECT` int
);

insert into test_field_names(id, low_case, upcase, camelcase, `table`, `array`, `select`)
values(1,0,0,0,0,0,0);

drop table if exists test_complex_types;
create table test_complex_types (
   id int,
   int_array array<int>,
   string_array array<string>,
   int_map map<int,string>,
   string_map map<string,string>,
   struct_field struct<id:int,name:string,description:string>
) row format delimited fields terminated by '\t' collection items terminated by ',' map keys terminated by ':';

load data local inpath '/data_maps.txt' into table test_complex_types;
