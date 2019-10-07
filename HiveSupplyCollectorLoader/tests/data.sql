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

insert into test_complex_types(id, int_array, string_array, int_map, string_map, struct_field)
select 1, array(1,2,3), array('one','two','three'), map(1, 'one', 2, 'two', 3, 'three'), map('one', 'two', 'two', 'three', 'three', 'four'), named_struct('id', 1, 'name', 'user', 'description', 'test user');
insert into test_complex_types(id, int_array, string_array, int_map, string_map, struct_field)
select 2, array(1,2,3), array('one','two','three'), map(1, 'one', 2, 'two', 3, 'three'), map('one', 'two', 'two', 'three', 'three', 'four'), named_struct('id', 1, 'name', 'user', 'description', 'test user');
insert into test_complex_types(id, int_array, string_array, int_map, string_map, struct_field)
select 3, array(1,2,3), array('one','two','three'), map(1, 'one', 2, 'two', 3, 'three'), map('one', 'two', 'two', 'three', 'three', 'four'), named_struct('id', 1, 'name', 'user', 'description', 'test user');
insert into test_complex_types(id, int_array, string_array, int_map, string_map, struct_field)
select 4, array(1,2,3), array('one','two','three'), map(1, 'one', 2, 'two', 3, 'three'), map('one', 'two', 'two', 'three', 'three', 'four'), named_struct('id', 1, 'name', 'user', 'description', 'test user');
insert into test_complex_types(id, int_array, string_array, int_map, string_map, struct_field)
select 5, array(1,2,3), array('one','two','three'), map(1, 'one', 2, 'two', 3, 'three'), map('one', 'two', 'two', 'three', 'three', 'four'), named_struct('id', 1, 'name', 'user', 'description', 'test user');
insert into test_complex_types(id, int_array, string_array, int_map, string_map, struct_field)
select 6, array(1,2,3), array('one','two','three'), map(1, 'one', 2, 'two', 3, 'three'), map('one', 'two', 'two', 'three', 'three', 'four'), named_struct('id', 1, 'name', 'user', 'description', 'test user');
