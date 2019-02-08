/***Create Hive tables and load data in text file format***/


hive

//hive metadata called as metastore
hive metastore


create database matymar7_retail_db_txt;

use matymar7_retail_db_txt;

show tables; 

exit;

//check hive-site.xml
//cd /etc/hive/conf
//search for the following
//warehouse.dir
//copy hive.metastore.warehouse.dir
//go back to hive


hive
use matymar7_retail_db_txt;
set hive.metastore.warehouse.dir; 


//check the databases
hive dfs -ls /apps/hive/warehouse;


create table orders ( 
	order_id int, 
	order_date string, 
	order_custamer_id int, 
	order_status string 
) row format delimited fields terminated by ','
stored as textfile; 


show tables; 

select * frm orders limit 10;

//With load statement we can load the data from file to our hive db

load data local inpath '/data/retail_db/orders' into table orders; 

//Now our data is loaded

select * frm orders limit 10;

//create similar table for order_items


create table order_items ( 
	order_item_id int, 
	order_item_order_id int, 
	order_item_product_id int, 
	order_item_quantity int, 
	order_item_subtotal float, 
	order_item_product_price float 
) row format delimited fields terminated by ','
stored as textfile; 


load data local inpath '/data/retail_db/order_items' into table order_items; 




/***Create Hive tables and load data in ORC file format***/

create database matymar7_retail_db_orc; 
use matymar7_retail_db_orc;

create table orders ( 
	order_id int, 
	order_date string, 
	order_custamer_id int, 
	order_status string 
) stored as orc; 

insert into table orders select * from  matymar7_retail_db_txt.orders;

create table order_items ( 
	order_item_id int, 
	order_item_order_id int, 
	order_item_product_id int, 
	order_item_quantity int, 
	order_item_subtotal float, 
	order_item_product_price float 
) stored as orc; 
insert into table order_items select * from matymar7_retail_db_txt.order_items;

describe orders;
describe formatted orders;


//You can find the location of the table as well
//i.e
Location: hdfs://nn01.itversity.com:8020/apps/htve/warehouse/dgadiroju_retail_db_orc.db/orders

dfs -is hdfs://nn01.itversity.com:8020/apps/hive/warehouse/dgodiroju_retaiLdb_orc.db/orders;


/***Using spark-shell to run Hive queries***/

spark-shell --master yarn --conf spark.ui.port=12345

//sc shows the SparkContext
sc
//use sqlContext
sqlContext

sqlContext.sql("use matymar7_retail_db_txt")

sqlContext.sql("show tables") 
sqlContext.sql("show tables").show 

sqtContext.sqt("setect * from orders limit 10").show


/***Functions - String, Date, Aggregations, Case, Row level transformations***/

hive;
show functions;

describe function length;

select length('Hello World'); 

select order_status, length(order_status) from orders limit 100;

/***String Functions */

create table customers ( 
	customer_id int, 
	customer_fname varchar(45), 
	customer_lname varchar(45), 
	customer_email varchar(45), 
	customer_password varchar(45), 
	customer_street varchar(255), 
	customer_city varchar(45), 
	customer_state varchar(45), 
	customer_zipcode varchar(45) 
) row format delimited fields terminated by ','
stored as textfile; 

load data local inpath 'data/ retail_db/customers' into table customers; 

/*
The most frequently used string functions
	substr or substring 
	instr 
	like 
	rtike 
	length 
	Icase or tower 
	ucase or upper 
	initcap 
	trim, ltrim, rtrim 
	cast 
	Ipad, rpad 
	sp lit 
	concat
*/

describe function substr; 
describe function substring; 

select substr('Hello World, How are you ', 14)
select substr('Hello World, How are you ', 6,5)
select substr('Hello World, How are you ', 7,5)
select substr('Hello World, How are you ', -3)
select substr('Hello World, How are you ', -7,3)
select substr('Hello World, How are you ', ' ')
select substr('Hello World, How are you ', 'World')


select "Hello World, How are you" like 'Hello';
select "Hello World, How are you" like 'Hello%'; 
select "Hello World, How are you" like 'World%'; 
select "Hello World, How are you" like '%World%'; 
select "Hello World, How are you" rlike '%World%';

select length("Hello World");
select lower("Hello World");
select lcase("Hello World");
select ucase("Hello World");

//Initcap  To convert each word to start with capital letter

describe function trim;



select trim(' hello world '),length(trim(' hello world '));
describe function lpad;

select lpad(12,2,'0') 
select lpad(2,2,'0')
select lpad(2,12,'0')
select lpad(12,12,'0')

//Cast Database example

select cast(substr(order_date, 6, 2) as int) from orders limit 10; 
select cast("hello" as int);

//split example

select split("Hello World, how are you", ' ')

//Index example
//If you want to get 5 elements after split means 4, if first means 0 element in our list

select index(split("Hello World, how are you", ' '), 4)
select index(split("Hello World, how are you", ' '), 0)


/***Date*/

/*The most frequently used date functions
	current_date 
	current_timestamp 
	date_add 
	date_format 
	date_sub 
	datediff 
	day 
	dayofmonth 
	to_date 
	to_unix_timestamp 
	to_utc_timestamp 
	from_unixtime 
	from_utc_timestamp 
	minute 
	month 
	months_between
	next_day 
*/

select current_date; 
select current_timestamp, 
select date_format(current_date, 'y'); 
select date_format(current_date, 'd'); 
select date_format(current_date, 'D'); 
select day(current_date); 

select dayofmonth(current_date)
select day('2017-10-09')
select current_date;
select current_timestamp;

select to_date(current_date)
select to_unix_timestamp(current_date)
select from_unixtime(1507348800)
select to_date(from_unixtime(1507348800))

select to_date(order_date) from orders limit 10;

/***Aggregations*/


select count(1) from orders;

select sum(order_item_subtotal) from order_items;

select count(1), count(distinct order_status) from orders;

/***Case*/

describe function case;

select case order_status 
		when 'CLOSED' then 'No Action' 
		when 'COMPLETE' then 'No Action' 
		end from orders limit 10; 

select order_status, 
	case order_status 
		when 'CLOSED' then 'NO Action' 
		When 'COMPLETE' then 'NO Action' 
		when 'ON_HOLD' then 'Pending Action' 
		when 'PAYMENT_REVIEW' then 'Pending Action' 
		when 'PENDING' then 'Pending Action' 
		When 'PENDING_PAYMENT' then 'Pending Action' 
		else 'Risky' 
	end	from orders limit 10; 


select order_status, 
	case order_status 
		when order_status in ('CLOSED','COMPLETE') then 'NO Action' 
		when order_status in ('ON_HOLD', 'PAYMENT_REVIEW','PENDING','PENDING_PAYMENT') then 'Pending Action' 
		else 'Risky' 
	end	from orders limit 10; 

/***NVL function*/
//If order_status is null, then the records will be "Status Missing" if not it will be order_status

select nvl(order_status, 'Status Missing') from orders limit 100;


/***Row level transformations*/


select * from orders limit 10; 

select concat(substr(order_date,1,4), substr(order_date,6,2)) from orders limit 10; 
select cast(concat(substr(order_date,1,4), substr(order_date,6,2)) as int) from orders limit 10; 

select cast(date_format(order_date, 'YYYYMM') as int) from orders limit 100; 


/***Joins*/

select o.*, c.* from orders o, customers c 
where o.customer_id = c.customer_id 
limit l0; 

select o.*, c.* from orders o inner join customers c 
on o.customer_id = c.customer_id 
limit l0; 

select o.*, c.* from orders o left outer join customers c 
on o.customer_id = c.customer_id 
limit l0; 

select count(1) from orders o inner join customers c 
on o.customer_id = c.customer_id 

select count(1) from orders o left join customers c 
on o.customer_id = c.customer_id 

select c.* from customers c left outer join orders o 
on o.customer_id = c.customer_id 
where o.order_custamer_id is null;

select * from where customer_id not in (select distinct from orders) 


/***Aggregations*/

select order_status, count(l) as order_count from orders group by order_status; 

select o.order_id, o.order_date, o.order_status, sum(oi.order_item_subtotal) order_revenue 
from orders o join order_items oi
on o.order_id = oi.order_item_order_id 
where o.order_status in ('COMPLETE', 'CLOSED')
group by o.order_id, o.order_date, o.order_status
having sum(oi.order_item_subtotal) >= 1000;


select o.order_date, round(sum(order_item_subtotal)) daily_revenue 
from orders o join order_items oi 
on o.order_id = oi.order_item_order_id
where o.order_status in ('COMPLETE', 'CLOSED')
group by order_date; 


/***Sorting*/

select o.order_id, o.order_date, o.order_status, round(sum(oi.order_item_subtotal) order_revenue 
from orders o join order_items oi 
on o.order_id = oi.order_item_order_id
where o.order_status in ( 'COMPLETE' , 'CLOSED')
group by o.order_id, o.order_date, o.order_status 
having sum(oi.order_item_subtotal) >=1000 
order by o.order_date, order_revenue desc; 

//*Distribute by sorting*/

select o.order_id, o.order_date, o.order_status, round(sum(oi.order_item_subtotal) order_revenue 
from orders o join order_items oi 
on o.order_id = oi.order_item_order_id
where o.order_status in ( 'COMPLETE' , 'CLOSED')
group by o.order_id, o.order_date, o.order_status 
having sum(oi.order_item_subtotal) >=1000 
distribute by o.order_date sort by o.order_date, order_revenue desc;


/***Set Operations*/


select 1, "Hello"
union all
select 2, "Hello"
union all
select 3, "Hello"


select 1, "Hello"
union 
select 2, "Hello"
union 
select 3, "Hello"


/***Analytics functions*/

/*
RANK
ROW_NUMBER
DENSE_RANK
CUME_DIST
PERCENT_RANK
NTILE
*/

select o.order_id, o. order_date, o. order_status, oi.order_item_subtotal,
round(sum(oi.order_item_subtotal) over (partition by o.order_id), 2) order_revenue, 
oi.order_item_subtotal/round(sum(oi.order_item_subtotal) over (partition by o.order_id) pct_revenue
from orders o join order_items oi 
on o.order_id = oi.order_item_order_id 
where o.order_status in ( 'COMPLETE' , 'CLOSED')
order by o.order_date, order_revenue desc; 


select * from ( 
select o.order_id, o.order_date, o.order_status, oi.order_item_subtotal,
round(sum(oi.order_item_subtotal) over (partition by o.order_id), 2) order_revenue, 
oi.order_item_subtotal/round(sum(oi.order_item_subtotal) over (partition by o.order_id) pct_revenue
from orders o join order_items oi 
on o.order_id =oi.order_item_order_id 
where o.order_status in ( 'COMPLETE' , 'CLOSED')) q
where order_revenue >=1000 
order by order_date, order_revenue desc; 


select * from ( 
select o.order_id, o.order_date, o.order_status, oi.order_item_subtotal,
round(sum(oi.order_item_subtotal) over (partition by o.order_id), 2) order_revenue, 
oi.order_item_subtotal/round(sum(oi.order_item_subtotal) over (partition by o.order_id) pct_revenue,
round(avg(oi.order_item_subtotal) over (partition by o.order_id), 2) avg_revenue 
from orders o join order_items oi 
on o.order_id =oi.order_item_order_id 
where o.order_status in ( 'COMPLETE' , 'CLOSED')) q
where order_revenue >=1000 
order by order_date, order_revenue desc; 


/*ranking*/

select * from ( 
select o.order_id, o.order_date, o.order_status, oi.order_item_subtotal,
round(sum(oi.order_item_subtotal) over (partition by o.order_id), 2) order_revenue, 
oi.order_item_subtotal/round(sum(oi.order_item_subtotal) over (partition by o.order_id) pct_revenue,
round(avg(oi.order_item_subtotal) over (partition by o.order_id), 2) avg_revenue 
rank() over(partition by o.order_id order by oi.order_item_subtotal desc) rnk_revenue,
dense_rank() over(partition by o.order_id order by oi.order_item_subtotal desc) dense_rnk_revenue,
percent_rank() over(partition by o.order_id order by oi.order_item_subtotal desc) pct_rnk_revenue,
row_number() over(partition by o.order_id order by oi.order_item_subtotal desc) rn_orderby_revenue,
row_number() over(partition by o.order_id) rn_revenue
from orders o join order_items oi 
on o.order_id =oi.order_item_order_id 
where o.order_status in ( 'COMPLETE' , 'CLOSED')) q
where order_revenue >=1000 
order by order_date, order_revenue desc, rnk_revenue;


/***Windowing functions*/

/*
LEAD
LAG
FIRST_VALUE
LAST_VALUE
*/


select * from ( 
select o.order_id, o.order_date, o.order_status, oi.order_item_subtotal,
round(sum(oi.order_item_subtotal) over (partition by o.order_id), 2) order_revenue, 
oi.order_item_subtotal/round(sum(oi.order_item_subtotal) over (partition by o.order_id) pct_revenue,
round(avg(oi.order_item_subtotal) over (partition by o.order_id), 2) avg_revenue 
rank() over(partition by o.order_id order by oi.order_item_subtotal desc) rnk_revenue,
dense_rank() over(partition by o.order_id order by oi.order_item_subtotal desc) dense_rnk_revenue,
percent_rank() over(partition by o.order_id order by oi.order_item_subtotal desc) pct_rnk_revenue,
row_number() over(partition by o.order_id order by oi.order_item_subtotal desc) rn_orderby_revenue,
row_number() over(partition by o.order_id) rn_revenue,
lead(oi.order_item_subtotal) over(partition by o.order_id order by oi.order_item_subtotal desc)
lead_order_item_subtotal,
lag(oi.order_item_subtotal) over(partition by o.order_id order by oi.order_item_subtotal desc)
lag_order_item_subtotal,
first_value(oi.order_item_subtotal) over(partition by o.order_id order by oi.order_item_subtotal desc)
first_order_item_subtotal,
last_value(oi.order_item_subtotal) over(partition by o.order_id order by oi.order_item_subtotal desc)
last_order_item_subtotal

from orders o join order_items oi 
on o.order_id =oi.order_item_order_id 
where o.order_status in ( 'COMPLETE' , 'CLOSED')) q
where order_revenue >=1000 
order by order_date, order_revenue desc, rnk_revenue;



select * from ( 
select o.order_id, o.order_date, o.order_status, oi.order_item_subtotal,
round(sum(oi.order_item_subtotal) over (partition by o.order_id), 2) order_revenue, 
oi.order_item_subtotal/round(sum(oi.order_item_subtotal) over (partition by o.order_id) pct_revenue,
round(avg(oi.order_item_subtotal) over (partition by o.order_id), 2) avg_revenue 
rank() over(partition by o.order_id order by oi.order_item_subtotal desc) rnk_revenue,
dense_rank() over(partition by o.order_id order by oi.order_item_subtotal desc) dense_rnk_revenue,
percent_rank() over(partition by o.order_id order by oi.order_item_subtotal desc) pct_rnk_revenue,
row_number() over(partition by o.order_id order by oi.order_item_subtotal desc) rn_orderby_revenue,
row_number() over(partition by o.order_id) rn_revenue,
lead(oi.order_item_subtotal) over(partition by o.order_id order by oi.order_item_subtotal desc)
lead_order_item_subtotal,
lag(oi.order_item_subtotal) over(partition by o.order_id order by oi.order_item_subtotal desc)
lag_order_item_subtotal,
first_value(oi.order_item_subtotal) over(partition by o.order_id order by oi.order_item_subtotal desc)
first_order_item_subtotal - oi.order_item_subtotal,
last_value(oi.order_item_subtotal) over(partition by o.order_id order by oi.order_item_subtotal desc)
last_order_item_subtotal

from orders o join order_items oi 
on o.order_id =oi.order_item_order_id 
where o.order_status in ( 'COMPLETE' , 'CLOSED')) q
where order_revenue >=1000 
order by order_date, order_revenue desc, rnk_revenue;