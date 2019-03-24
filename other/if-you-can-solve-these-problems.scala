http://discuss.itversity.com/t/if-you-can-solve-these-problems-you-may-be-ready-for-cca-175-give-it-a-shot/12529


## preparation local  
mkdir jay 
cd jay

mysql -u retail_dba -p cludera



##Problem1
===========================================================
// Import orders table from mysql (db: retail_db , user : retail_user , password : xxxx)
// Import only records that are in “COMPLETE” status
// Import all columns other than customer id
// Save the imported data as text and tab delimitted in this hdfs location /user/cloudera/jay/problem1/
===========================================================

// mkdir problem1
// cd problem1


// mysql> describe orders;
// +-------------------+-------------+------+-----+---------+----------------+
// | Field             | Type        | Null | Key | Default | Extra          |
// +-------------------+-------------+------+-----+---------+----------------+
// | order_id          | int(11)     | NO   | PRI | NULL    | auto_increment |
// | order_date        | datetime    | NO   |     | NULL    |                |
// | order_customer_id | int(11)     | NO   |     | NULL    |                |
// | order_status      | varchar(45) | NO   |     | NULL    |                |
// +-------------------+-------------+------+-----+---------+----------------+

sqoop eval \
--connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" \
--username "retail_dba" \
--password "cloudera" \
--query "select count(*) from orders"

sqoop import \
--connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" \
--username "retail_dba" \
--password "cloudera" \
--table orders \
--columns "order_id,order_date,order_status" \
--where "order_status='COMPLETE'" \
--target-dir "/user/cloudera/jay/problem1" \
--fields-terminated-by '\t' \
--as-textfile




// [cloudera@quickstart problem1]$ ls
// orders.java
// [cloudera@quickstart problem1]$ hdfs dfs -ls /user/cloudera/jay/problem1
// Found 5 items
// -rw-r--r--   1 cloudera cloudera          0 2019-03-17 08:25 /user/cloudera/jay/problem1/_SUCCESS
// -rw-r--r--   1 cloudera cloudera     207781 2019-03-17 08:25 /user/cloudera/jay/problem1/part-m-00000
// -rw-r--r--   1 cloudera cloudera     209272 2019-03-17 08:25 /user/cloudera/jay/problem1/part-m-00001
// -rw-r--r--   1 cloudera cloudera     216154 2019-03-17 08:25 /user/cloudera/jay/problem1/part-m-00002
// -rw-r--r--   1 cloudera cloudera     210382 2019-03-17 08:25 /user/cloudera/jay/problem1/part-m-00003
// [cloudera@quickstart problem1]$ 







##Problem2
===========================================================
// Import orders table from mysql (db: retail_db , user : retail_user , password : xxxx)
// Import all records and columns from Orders table
// Save the imported data as text and tab delimitted in this hdfs location /user/cloudera/jay/problem2/
===========================================================


// [cloudera@quickstart problem1]$ cd ..
// [cloudera@quickstart jay]$ mkdir problem2
// [cloudera@quickstart jay]$ cd problem2



sqoop import \
--connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" \
--username "retail_dba" \
--password "cloudera" \
--table orders \
--target-dir /user/cloudera/jay/problem2 \
--fields-terminated-by '\t' \
--as-textfile



// [cloudera@quickstart problem2]$ ls
// orders.java
// [cloudera@quickstart problem2]$ hdfs dfs -ls /user/cloudera/jay/problem2
// Found 5 items
// -rw-r--r--   1 cloudera cloudera          0 2019-03-17 08:28 /user/cloudera/jay/problem2/_SUCCESS
// -rw-r--r--   1 cloudera cloudera     741614 2019-03-17 08:28 /user/cloudera/jay/problem2/part-m-00000
// -rw-r--r--   1 cloudera cloudera     753022 2019-03-17 08:28 /user/cloudera/jay/problem2/part-m-00001
// -rw-r--r--   1 cloudera cloudera     752368 2019-03-17 08:28 /user/cloudera/jay/problem2/part-m-00002
// -rw-r--r--   1 cloudera cloudera     752940 2019-03-17 08:28 /user/cloudera/jay/problem2/part-m-00003
// [cloudera@quickstart problem2]$






##Problem3
===========================================================
// Export orders data into mysql
// Input Source : /user/cloudera/jay/problem2/
// Target Table : Mysql . DB = retail_db . Table Name : jay_orders
// Reason for somealias in table name is … to not overwrite others in mysql db in labs
===========================================================

// [cloudera@quickstart problem2]$ cd ..
// [cloudera@quickstart jay]$ mkdir problem3
// [cloudera@quickstart jay]$ cd problem3



// mysql -u retail_dba -p
// use retail_db;
// create table jay_orders (
// order_id int primary key auto_increment,
// order_date datetime not null,
// order_customer_id int not null,
// order_status varchar(45) not null
// );

// create table jay_orders as select * from orders;
// truncate table jay_orders;

// drop table jay_orders;


// +-------------------+-------------+------+-----+---------+----------------+
// | Field             | Type        | Null | Key | Default | Extra          |
// +-------------------+-------------+------+-----+---------+----------------+
// | order_id          | int(11)     | NO   | PRI | NULL    | auto_increment |
// | order_date        | datetime    | NO   |     | NULL    |                |
// | order_customer_id | int(11)     | NO   |     | NULL    |                |
// | order_status      | varchar(45) | NO   |     | NULL    |                |
// +-------------------+-------------+------+-----+---------+----------------+



sqoop export \
--connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" \
--username "retail_dba" \
--password "cloudera" \
--table jay_orders \
--export-dir "/user/cloudera/jay/problem2" \
--input-fields-terminated-by '\t'





##Problem4
===========================================================
// Read data from hive and perform transformation and save it back in HDFS
// Read table populated from Problem 3 (jay__mock_orders )
// Produce output in this format (2 fields) , sort by order count in descending and save it as avro with snappy compression in hdfs location /user/cloudera/jay/problem4/avro-snappy
// ORDER_STATUS : ORDER_COUNT
// COMPLETE 54
// CANCELLED 89
// INPROGRESS 23
// Save above output in avro snappy compression in avro format in hdfs location /user/cloudera/jay/problem4/avro
===========================================================

spark-shell --master yarn \
--conf spark.ui.port=12654 \
--num-executors 1 \
--executor-memory 512M


//we don't store in problem2 in hive, but if we have hive source this is the solution

// sudo ln -s /etc/hive/conf.dist/hive-context.xml /etc/spark/conf/hive-context.xml
// var hc = new org.apache.spark.sql.hive.HiveContext(sc);
// var dataFile = hc.sql("Select order_status as ORDER_STATUS, count(distinct order_id) as ORDER_COUNT from jay__mock_orders group by order_status order by ORDER_COUNT desc");



import com.databricks.spark.avro._;
var dataFile = sc.textFile("/user/cloudera/jay/problem2")

var dataDF = dataFile.
map(_.split("\t")).
map(rec=>(rec(0).trim.toInt,rec(1),rec(2).toInt,rec(3).trim)).
toDF("order_id","order_date","order_customer_id","order_status")


dataDF.registerTempTable("jay__mock_orders")

var dataResult4 = sqlContext.sql({"""
Select 
order_status as ORDER_STATUS, 
count(distinct order_id) as ORDER_COUNT 
from jay__mock_orders 
group by order_status
order by ORDER_COUNT desc"""})

dataResult4.show()

// sqlContext.setConf("spark.sql.avro.compression.codec","SnappyCodec") didn't work for me on spark 1.6

sqlContext.setConf("spark.sql.avro.compression.codec","snappy")
dataResult4.write.avro("/user/cloudera/jay/problem4/avro_snappy")



// [cloudera@quickstart problem5]$ hdfs dfs -ls /user/cloudera/jay/problem4/avro_snappy
// Found 11 items
// -rw-r--r--   1 cloudera cloudera          0 2019-03-17 10:27 /user/cloudera/jay/problem4/avro_snappy/_SUCCESS
// -rw-r--r--   1 cloudera cloudera        239 2019-03-17 10:27 /user/cloudera/jay/problem4/avro_snappy/part-r-00000-3e9e332a-49ef-46ef-8a38-3a259a7d8c72.avro
// -rw-r--r--   1 cloudera cloudera        246 2019-03-17 10:27 /user/cloudera/jay/problem4/avro_snappy/part-r-00001-3e9e332a-49ef-46ef-8a38-3a259a7d8c72.avro
// -rw-r--r--   1 cloudera cloudera        241 2019-03-17 10:27 /user/cloudera/jay/problem4/avro_snappy/part-r-00002-3e9e332a-49ef-46ef-8a38-3a259a7d8c72.avro
// -rw-r--r--   1 cloudera cloudera        237 2019-03-17 10:27 /user/cloudera/jay/problem4/avro_snappy/part-r-00003-3e9e332a-49ef-46ef-8a38-3a259a7d8c72.avro
// -rw-r--r--   1 cloudera cloudera        236 2019-03-17 10:27 /user/cloudera/jay/problem4/avro_snappy/part-r-00004-3e9e332a-49ef-46ef-8a38-3a259a7d8c72.avro
// -rw-r--r--   1 cloudera cloudera        237 2019-03-17 10:27 /user/cloudera/jay/problem4/avro_snappy/part-r-00005-3e9e332a-49ef-46ef-8a38-3a259a7d8c72.avro
// -rw-r--r--   1 cloudera cloudera        245 2019-03-17 10:27 /user/cloudera/jay/problem4/avro_snappy/part-r-00006-3e9e332a-49ef-46ef-8a38-3a259a7d8c72.avro
// -rw-r--r--   1 cloudera cloudera        238 2019-03-17 10:27 /user/cloudera/jay/problem4/avro_snappy/part-r-00007-3e9e332a-49ef-46ef-8a38-3a259a7d8c72.avro
// -rw-r--r--   1 cloudera cloudera        244 2019-03-17 10:27 /user/cloudera/jay/problem4/avro_snappy/part-r-00008-3e9e332a-49ef-46ef-8a38-3a259a7d8c72.avro
// -rw-r--r--   1 cloudera cloudera        201 2019-03-17 10:27 /user/cloudera/jay/problem4/avro_snappy/part-r-00009-3e9e332a-49ef-46ef-8a38-3a259a7d8c72.avro
// [cloudera@quickstart problem5]$ 




+-------------------+-------------+------+-----+---------+----------------+
| Field             | Type        | Null | Key | Default | Extra          |
+-------------------+-------------+------+-----+---------+----------------+
| order_id          | int(11)     | NO   | PRI | NULL    | auto_increment |
| order_date        | datetime    | NO   |     | NULL    |                |
| order_customer_id | int(11)     | NO   |     | NULL    |                |
| order_status      | varchar(45) | NO   |     | NULL    |                |
+-------------------+-------------+------+-----+---------+----------------+



sqlContext.setConf("spark.sql.avro.compression.codec","uncompressed")
dataResult4.write.avro("/user/cloudera/jay/problem4/avro")


// [cloudera@quickstart problem5]$ hdfs dfs -ls /user/cloudera/jay/problem4/avro
// Found 11 items
// -rw-r--r--   1 cloudera cloudera          0 2019-03-17 10:26 /user/cloudera/jay/problem4/avro/_SUCCESS
// -rw-r--r--   1 cloudera cloudera        231 2019-03-17 10:26 /user/cloudera/jay/problem4/avro/part-r-00000-753980c0-e870-4765-8e75-e5722e19c7d8.avro
// -rw-r--r--   1 cloudera cloudera        238 2019-03-17 10:26 /user/cloudera/jay/problem4/avro/part-r-00001-753980c0-e870-4765-8e75-e5722e19c7d8.avro
// -rw-r--r--   1 cloudera cloudera        233 2019-03-17 10:26 /user/cloudera/jay/problem4/avro/part-r-00002-753980c0-e870-4765-8e75-e5722e19c7d8.avro
// -rw-r--r--   1 cloudera cloudera        229 2019-03-17 10:26 /user/cloudera/jay/problem4/avro/part-r-00003-753980c0-e870-4765-8e75-e5722e19c7d8.avro
// -rw-r--r--   1 cloudera cloudera        228 2019-03-17 10:26 /user/cloudera/jay/problem4/avro/part-r-00004-753980c0-e870-4765-8e75-e5722e19c7d8.avro
// -rw-r--r--   1 cloudera cloudera        229 2019-03-17 10:26 /user/cloudera/jay/problem4/avro/part-r-00005-753980c0-e870-4765-8e75-e5722e19c7d8.avro
// -rw-r--r--   1 cloudera cloudera        237 2019-03-17 10:26 /user/cloudera/jay/problem4/avro/part-r-00006-753980c0-e870-4765-8e75-e5722e19c7d8.avro
// -rw-r--r--   1 cloudera cloudera        230 2019-03-17 10:26 /user/cloudera/jay/problem4/avro/part-r-00007-753980c0-e870-4765-8e75-e5722e19c7d8.avro
// -rw-r--r--   1 cloudera cloudera        236 2019-03-17 10:26 /user/cloudera/jay/problem4/avro/part-r-00008-753980c0-e870-4765-8e75-e5722e19c7d8.avro
// -rw-r--r--   1 cloudera cloudera        199 2019-03-17 10:26 /user/cloudera/jay/problem4/avro/part-r-00009-753980c0-e870-4765-8e75-e5722e19c7d8.avro





##Problem5 
===========================================================
// Import orders table from mysql (db: retail_db , user : retail_user , password : xxxx)
// Import all records and columns from Orders table
// Save the imported data as avro and snappy compression in hdfs location /user/cloudera/jay/problem5/avro-snappy/

// Read above hdfs data
// Consider orders only in “COMPLETE” status and order id between 1000 and 50000 (1001 to 49999)
// Save the output (only 2 columns orderid and orderstatus) in parquet format with gzip compression in location /user/cloudera/jay/problem5/parquet-gzip/
// Advance : Try if you can save output only in 2 files (Tip : use coalesce(2))
===========================================================

// mkdir problem5
// cd problem5

//after sqoop
//[cloudera@quickstart problem5]$ ls
//orders.avsc  orders.java


sqoop import \
--connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" \
--username "retail_dba" \
--password "cloudera" \
--table orders \
--target-dir /user/cloudera/jay/problem5/avro-snappy/ \
--compress \
--compression-codec org.apache.hadoop.io.compress.SnappyCodec \
--as-avrodatafile

// [cloudera@quickstart problem5]$ hdfs dfs -ls /user/cloudera/jay/problem5/avro-snappy
// Found 5 items
// -rw-r--r--   1 cloudera cloudera          0 2019-03-17 10:20 /user/cloudera/jay/problem5/avro-snappy/_SUCCESS
// -rw-r--r--   1 cloudera cloudera     164090 2019-03-17 10:20 /user/cloudera/jay/problem5/avro-snappy/part-m-00000.avro
// -rw-r--r--   1 cloudera cloudera     164157 2019-03-17 10:20 /user/cloudera/jay/problem5/avro-snappy/part-m-00001.avro
// -rw-r--r--   1 cloudera cloudera     164278 2019-03-17 10:20 /user/cloudera/jay/problem5/avro-snappy/part-m-00002.avro
// -rw-r--r--   1 cloudera cloudera     169339 2019-03-17 10:20 /user/cloudera/jay/problem5/avro-snappy/part-m-00003.avro

sqoop import \
--connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" \
--username "retail_dba" \
--password "cloudera" \
--table orders \
--target-dir /user/cloudera/jay/problem5/avro/ \
--as-avrodatafile


// [cloudera@quickstart problem5]$ hdfs dfs -ls /user/cloudera/jay/problem5/avro
// Found 5 items
// -rw-r--r--   1 cloudera cloudera          0 2019-03-17 10:23 /user/cloudera/jay/problem5/avro/_SUCCESS
// -rw-r--r--   1 cloudera cloudera     439146 2019-03-17 10:23 /user/cloudera/jay/problem5/avro/part-m-00000.avro
// -rw-r--r--   1 cloudera cloudera     447726 2019-03-17 10:23 /user/cloudera/jay/problem5/avro/part-m-00001.avro
// -rw-r--r--   1 cloudera cloudera     446959 2019-03-17 10:23 /user/cloudera/jay/problem5/avro/part-m-00002.avro
// -rw-r--r--   1 cloudera cloudera     447606 2019-03-17 10:23 /user/cloudera/jay/problem5/avro/part-m-00003.avro



import com.databricks.spark.avro._;

var dataFile = sqlContext.read.avro("/user/cloudera/jay/problem5/avro-snappy/")

dataFile.registerTempTable("orders")

sqlContext.sql("select * from orders").show()

var resultData = sqlContext.sql({"""
select 
order_id,order_status 
from orders 
where order_status='COMPLETE' 
and (order_id between 1000 and 50000)"""})

sqlContext.setConf("spark.sql.parquet.compression.codec","gzip")
resultData.coalesce(2).write.parquet("/user/cloudera/jay/problem5/parquet-gzip/")


sqlContext.setConf("spark.sql.parquet.compression.codec","uncompressed")
resultData.coalesce(2).write.parquet("/user/cloudera/jay/problem5/parquet-un/")

##coalesce





##Problem6
===========================================================
// Import orders table from mysql (db: retail_db , user : retail_user , password : xxxx)
// Import all records and columns from Orders table
// Save the imported data as text and tab delimitted in this hdfs location /user/cloudera/jay/problem6/orders/

// Import order_items table from mysql (db: retail_db , user : retail_user , password : xxxx)
// Import all records and columns from Order_items table
// Save the imported data as text and tab delimitted in this hdfs location /user/cloudera/jay/problem6/order-items/

// Read orders data from above HDFS location
// Read order items data form above HDFS location
// Produce output in this format (price and total should be treated as decimals)
// Consider only CLOSED & COMPLETE orders
// ORDER_ID ORDER_ITEM_ID PRODUCT_PRICE ORDER_SUBTOTAL ORDER_TOTAL

// Note : ORDER_TOTAL = combined total price for this order

// Save above output as ORC in hive table “jay_mock_orderdetails”
// (Tip : Try saving into hive table from DF directly without explicit table creation manually)

// Note : This problem updated on Jun 4 with more details to reduce ambiguity based on received feedback/comments from users. (Thank You )
===========================================================

// mkdir problem6
// cd problem6
//[cloudera@quickstart problem5]$ ls
//orders.avsc  orders.java


sqoop import \
--connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" \
--username "retail_dba" \
--password "cloudera" \
--table orders \
--target-dir /user/cloudera/jay/problem6/orders/ \
--fields-terminated-by '\t' \
--as-textfile


sqoop import \
--connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" \
--username "retail_dba" \
--password "cloudera" \
--table order_items \
--target-dir /user/cloudera/jay/problem6/order-items/ \
--fields-terminated-by '\t' \
--as-textfile


// [cloudera@quickstart problem6]$ ls
// order_items.java  orders.java
// [cloudera@quickstart problem6]$ hdfs dfs -ls /user/cloudera/jay/problem6/orders | wc -l
// 6
// [cloudera@quickstart problem6]$ hdfs dfs -ls /user/cloudera/jay/problem6/order-items | wc -l
// 6


var ordersData = sc.textFile("/user/cloudera/jay/problem6/orders")
var ordersitemsData = sc.textFile("/user/cloudera/jay/problem6/order-items/")


var ordersMap = ordersData.
map(_.split("\t")).map(o=>(o(0),o(1),o(2),o(3))).
toDF("order_id","order_date","order_customer_id","order_status")

var orderitemsMap = ordersitemsData.
map(_.split("\t")).map(o=>(o(0),o(1),o(2),o(3),o(4),o(5))).
toDF("order_item_id","order_item_order_id","order_item_product_id","order_item_quantity","order_item_subtotal","order_item_product_price")


ordersMap.registerTempTable("orders")

orderitemsMap.registerTempTable("order_items")

var resultData = sqlContext.sql({"""
select 
o.order_id as ORDER_ID 
,oi.order_item_id as ORDER_ITEM_ID 
,oi.order_item_product_price as PRODUCT_PRICE 
,cast(oi.order_item_subtotal as decimal(10,2)) as ORDER_SUBTOTAL 
,count(1) as ORDER_TOTAL
from orders o 
inner join order_items oi 
on o.order_id=oi.order_item_order_id
where order_status in ('CLOSED','COMPLETE')
group by o.order_id,oi.order_item_id, oi.order_item_product_price, oi.order_item_subtotal
"""})


var resultData = sqlContext.sql({"""
create table default.jay_mock_orderdetails stored as orc as
select 
o.order_id as ORDER_ID 
,oi.order_item_id as ORDER_ITEM_ID 
,oi.order_item_product_price as PRODUCT_PRICE 
,cast(oi.order_item_subtotal as decimal(10,2)) as ORDER_SUBTOTAL 
,count(1) as ORDER_TOTAL
from orders o 
inner join order_items oi 
on o.order_id=oi.order_item_order_id
where order_status in ('CLOSED','COMPLETE')
group by o.order_id,oi.order_item_id, oi.order_item_product_price, oi.order_item_subtotal
"""})



// +-------------------+-------------+------+-----+---------+----------------+
// | Field             | Type        | Null | Key | Default | Extra          |
// +-------------------+-------------+------+-----+---------+----------------+
// | order_id          | int(11)     | NO   | PRI | NULL    | auto_increment |
// | order_date        | datetime    | NO   |     | NULL    |                |
// | order_customer_id | int(11)     | NO   |     | NULL    |                |
// | order_status      | varchar(45) | NO   |     | NULL    |                |
// +-------------------+-------------+------+-----+---------+----------------+


// +--------------------------+------------+------+-----+---------+----------------+
// | Field                    | Type       | Null | Key | Default | Extra          |
// +--------------------------+------------+------+-----+---------+----------------+
// | order_item_id            | int(11)    | NO   | PRI | NULL    | auto_increment |
// | order_item_order_id      | int(11)    | NO   |     | NULL    |                |
// | order_item_product_id    | int(11)    | NO   |     | NULL    |                |
// | order_item_quantity      | tinyint(4) | NO   |     | NULL    |                |
// | order_item_subtotal      | float      | NO   |     | NULL    |                |
// | order_item_product_price | float      | NO   |     | NULL    |                |
// +--------------------------+------------+------+-----+---------+----------------+















#Problem7 
===========================================================
// Import order_items table from mysql (db: retail_db , user : retail_user , password : xxxx)
// Import all records and columns from Order_items table
// Save the imported data as parquet in this hdfs location /user/cloudera/jay/problem7/order-items/

// Import products table from mysql (db: retail_db , user : retail_user , password : xxxx)
// Import all records and columns from products table
// Save the imported data as avro in this hdfs location /user/cloudera/jay/problem7/products/

// Read above orderitems and products from HDFS location
// Produce this output :

// ORDER_ITEM_ORDER_ID PRODUCT_ID PRODUCT_NAME PRODUCT_PRICE ORDER_SUBTOTAL

// Save above output as avro snappy in hdfs location /user/cloudera/jay/problem7/output-avro-snappy/

// Note : This problem updated on Jun 4 with more details to reduce ambiguity based on received feedback/comments from users. (Thank You )
===========================================================


// mkdir problem7
// cd problem7


//after sqoop
// [cloudera@quickstart problem7]$ ls
// codegen_order_items.java  products.avsc  products.java




sqoop import \
--connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" \
--username "retail_dba" \
--password "cloudera" \
--table order_items \
--target-dir /user/cloudera/jay/problem7/order_items \
--as-parquetfile



// [cloudera@quickstart problem7]$ hdfs dfs -ls /user/cloudera/jay/problem7/order_items
// Found 6 items
// drwxr-xr-x   - cloudera cloudera          0 2019-03-17 11:18 /user/cloudera/jay/problem7/order_items/.metadata
// drwxr-xr-x   - cloudera cloudera          0 2019-03-17 11:25 /user/cloudera/jay/problem7/order_items/.signals
// -rw-r--r--   1 cloudera cloudera     415317 2019-03-17 11:25 /user/cloudera/jay/problem7/order_items/040fdf92-cdba-48eb-9701-ffb906f93f17.parquet
// -rw-r--r--   1 cloudera cloudera     415542 2019-03-17 11:25 /user/cloudera/jay/problem7/order_items/1e97486c-46b9-4e50-afd9-2ee8eee552be.parquet
// -rw-r--r--   1 cloudera cloudera     415601 2019-03-17 11:24 /user/cloudera/jay/problem7/order_items/adabdde8-f07e-427b-8934-e4eaeb3356eb.parquet
// -rw-r--r--   1 cloudera cloudera     431779 2019-03-17 11:25 /user/cloudera/jay/problem7/order_items/de8aeb28-6b78-4f97-ad34-1d5793110185.parquet





sqoop import \
--connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" \
--username "retail_dba" \
--password "cloudera" \
--table products \
--target-dir /user/cloudera/jay/problem7/products/ \
--as-avrodatafile



// [cloudera@quickstart problem7]$ hdfs dfs -ls /user/cloudera/jay/problem7/products/
// Found 5 items
// -rw-r--r--   1 cloudera supergroup          0 2019-03-17 11:26 /user/cloudera/jay/problem7/products/_SUCCESS
// -rw-r--r--   1 cloudera supergroup      42658 2019-03-17 11:26 /user/cloudera/jay/problem7/products/part-m-00000.avro
// -rw-r--r--   1 cloudera supergroup      44748 2019-03-17 11:26 /user/cloudera/jay/problem7/products/part-m-00001.avro
// -rw-r--r--   1 cloudera supergroup      43151 2019-03-17 11:26 /user/cloudera/jay/problem7/products/part-m-00002.avro
// -rw-r--r--   1 cloudera supergroup      47535 2019-03-17 11:26 /user/cloudera/jay/problem7/products/part-m-00003.avro




import com.databricks.spark.avro._;

var orderitemsData = sqlContext.read.parquet("/user/cloudera/jay/problem7/order_items/")
var productsData = sqlContext.read.avro("/user/cloudera/jay/problem7/products")

orderitemsData.registerTempTable("order_items")
productsData.registerTempTable("products")

+---------------------+--------------+------+-----+---------+----------------+
| Field               | Type         | Null | Key | Default | Extra          |
+---------------------+--------------+------+-----+---------+----------------+
| product_id          | int(11)      | NO   | PRI | NULL    | auto_increment |
| product_category_id | int(11)      | NO   |     | NULL    |                |
| product_name        | varchar(45)  | NO   |     | NULL    |                |
| product_description | varchar(255) | NO   |     | NULL    |                |
| product_price       | float        | NO   |     | NULL    |                |
| product_image       | varchar(255) | NO   |     | NULL    |                |
+---------------------+--------------+------+-----+---------+----------------+

+--------------------------+------------+------+-----+---------+----------------+
| Field                    | Type       | Null | Key | Default | Extra          |
+--------------------------+------------+------+-----+---------+----------------+
| order_item_id            | int(11)    | NO   | PRI | NULL    | auto_increment |
| order_item_order_id      | int(11)    | NO   |     | NULL    |                |
| order_item_product_id    | int(11)    | NO   |     | NULL    |                |
| order_item_quantity      | tinyint(4) | NO   |     | NULL    |                |
| order_item_subtotal      | float      | NO   |     | NULL    |                |
| order_item_product_price | float      | NO   |     | NULL    |                |
+--------------------------+------------+------+-----+---------+----------------+


var dataResult = sqlContext.sql({""" 
select 
oi.order_item_order_id as ORDER_ITEM_ORDER_ID 
,p.product_id as PRODUCT_ID 
,p.product_name as PRODUCT_NAME 
,p.product_price as PRODUCT_PRICE 
,oi.order_item_subtotal as ORDER_SUBTOTAL
from order_items oi
inner join products p
on oi.order_item_product_id=p.product_id
"""});

sqlContext.setConf("spark.sql.avro.compression.codec","snappy")

dataResult.write.avro("/user/cloudera/jay/problem7/output-avro-snappy/")


// [cloudera@quickstart problem7]$ hdfs dfs -ls /user/cloudera/jay/problem7/output-avro-snappy
// Found 5 items
// -rw-r--r--   1 cloudera cloudera          0 2019-03-17 11:33 /user/cloudera/jay/problem7/output-avro-snappy/_SUCCESS
// -rw-r--r--   1 cloudera cloudera     374878 2019-03-17 11:33 /user/cloudera/jay/problem7/output-avro-snappy/part-r-00000-13036296-84f3-4997-b1bc-7defd28bac12.avro
// -rw-r--r--   1 cloudera cloudera     376579 2019-03-17 11:33 /user/cloudera/jay/problem7/output-avro-snappy/part-r-00001-13036296-84f3-4997-b1bc-7defd28bac12.avro
// -rw-r--r--   1 cloudera cloudera     373394 2019-03-17 11:33 /user/cloudera/jay/problem7/output-avro-snappy/part-r-00002-13036296-84f3-4997-b1bc-7defd28bac12.avro
// -rw-r--r--   1 cloudera cloudera     365427 2019-03-17 11:33 /user/cloudera/jay/problem7/output-avro-snappy/part-r-00003-13036296-84f3-4997-b1bc-7defd28bac12.avro





sqlContext.setConf("spark.sql.avro.compression.codec","uncompressed")

dataResult.write.avro("/user/cloudera/jay/problem7/output-avro-uncompressed/")



// [cloudera@quickstart problem7]$ hdfs dfs -ls /user/cloudera/jay/problem7/output-avro-uncompressed
// Found 5 items
// -rw-r--r--   1 cloudera cloudera          0 2019-03-17 11:34 /user/cloudera/jay/problem7/output-avro-uncompressed/_SUCCESS
// -rw-r--r--   1 cloudera cloudera    2375526 2019-03-17 11:34 /user/cloudera/jay/problem7/output-avro-uncompressed/part-r-00000-9adebbf1-082b-43c5-9ce3-a2ff10947656.avro
// -rw-r--r--   1 cloudera cloudera    2376071 2019-03-17 11:34 /user/cloudera/jay/problem7/output-avro-uncompressed/part-r-00001-9adebbf1-082b-43c5-9ce3-a2ff10947656.avro
// -rw-r--r--   1 cloudera cloudera    2352283 2019-03-17 11:34 /user/cloudera/jay/problem7/output-avro-uncompressed/part-r-00002-9adebbf1-082b-43c5-9ce3-a2ff10947656.avro
// -rw-r--r--   1 cloudera cloudera    2375405 2019-03-17 11:34 /user/cloudera/jay/problem7/output-avro-uncompressed/part-r-00003-9adebbf1-082b-43c5-9ce3-a2ff10947656.avro







#Problem8
===========================================================
// Read order item from /user/cloudera/jay/problem7/order_items/
// Read products from /user/cloudera/jay/problem7/products/

// Produce output that shows product id and total no. of orders for each product id.
// Output should be in this format… sorted by order count descending
// If any product id has no order then order count for that product id should be “0”

// PRODUCT_ID PRODUCT_PRICE ORDER_COUNT

// Output should be saved as sequence file with Key=ProductID , Value = PRODUCT_ID|PRODUCT_PRICE|ORDER_COUNT (pipe separated)
===========================================================



var orderitemsData = sqlContext.read.parquet("/user/cloudera/jay/problem7/order_items/")
var productsData = sqlContext.read.avro("/user/cloudera/jay/problem7/products/")

orderitemsData.registerTempTable("order_items")
productsData.registerTempTable("products")


var dataResult = sqlContext.sql({""" 
select 
p.product_id as PRODUCT_ID
,p.product_price as PRODUCT_PRICE  
,count(distinct(oi.order_item_order_id)) as TOTAL_NO_OF_ORDERS 
from order_items oi
inner join products p
on oi.order_item_product_id=p.product_id
group by p.product_id,p.product_price
order by TOTAL_NO_OF_ORDERS desc
"""});

var seqData = dataResult.toDF().map(rec=>(rec(0).toString,rec(0).toString+"|"+rec(1).toString+"|"+rec(2).toString))


seqData.take(5).foreach(println)

seqData.saveAsSequenceFile("/user/cloudera/jay/problem8/sequence/")


// [cloudera@quickstart problem7]$ hdfs dfs -ls /user/cloudera/jay/problem8/sequence
// Found 76 items
// -rw-r--r--   1 cloudera cloudera          0 2019-03-17 11:52 /user/cloudera/jay/problem8/sequence/_SUCCESS
// -rw-r--r--   1 cloudera cloudera        106 2019-03-17 11:52 /user/cloudera/jay/problem8/sequence/part-00000
// -rw-r--r--   1 cloudera cloudera        107 2019-03-17 11:52 /user/cloudera/jay/problem8/sequence/part-00001
// -rw-r--r--   1 cloudera cloudera        105 2019-03-17 11:52 /user/cloudera/jay/problem8/sequence/part-00002












#Problem9
===========================================================
// Import orders table from mysql (db: retail_db , user : retail_user , password : xxxx)
// Import all records and columns from Orders table
// Save the imported data as avro in this hdfs location /user/cloudera/jay/problem9/orders-avro/

// Read above Avro orders data
// Convert to JSON
// Save JSON text file in hdfs location /user/cloudera/jay/problem9/orders-json/

// Read json data from /user/cloudera/jay/problem9/orders-json/
// Consider only “COMPLETE” orders.
// Save orderid and order status (just 2 columns) as JSON text file in location /user/cloudera/jay/problem9/orders-mini-json/
===========================================================


// mkdir problem9
// cd problem9




sqoop import \
--connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" \
--username "retail_dba" \
--password "cloudera" \
--table orders \
--target-dir /user/cloudera/jay/problem9/orders-avro/ \
--as-avrodatafile


// [cloudera@quickstart problem9]$ ls
// orders.avsc  orders.java
// [cloudera@quickstart problem9]$ hdfs dfs -ls /user/cloudera/jay/problem9/orders-avro
// Found 5 items
// -rw-r--r--   1 cloudera cloudera          0 2019-03-17 11:55 /user/cloudera/jay/problem9/orders-avro/_SUCCESS
// -rw-r--r--   1 cloudera cloudera     439146 2019-03-17 11:55 /user/cloudera/jay/problem9/orders-avro/part-m-00000.avro
// -rw-r--r--   1 cloudera cloudera     447726 2019-03-17 11:55 /user/cloudera/jay/problem9/orders-avro/part-m-00001.avro
// -rw-r--r--   1 cloudera cloudera     446959 2019-03-17 11:55 /user/cloudera/jay/problem9/orders-avro/part-m-00002.avro
// -rw-r--r--   1 cloudera cloudera     447606 2019-03-17 11:55 /user/cloudera/jay/problem9/orders-avro/part-m-00003.avro





import com.databricks.spark.avro._;

var ordersData = sqlContext.read.avro("/user/cloudera/jay/problem9/orders-avro/")

ordersData.registerTempTable("orders")

var dataResult = sqlContext.sql("select order_id, order_status from orders where order_status='COMPLETE'")

dataResult.toJSON.saveAsTextFile("/user/cloudera/jay/problem9/orders-mini-json/")



// [cloudera@quickstart problem9]$ hdfs dfs -cat /user/cloudera/jay/problem9/orders-mini-json/part-00000
// {"order_id":10139,"order_status":"COMPLETE"}
// {"order_id":10147,"order_status":"COMPLETE"}
// {"order_id":10150,"order_status":"COMPLETE"}
// {"order_id":10151,"order_status":"COMPLETE"}
// {"order_id":10160,"order_status":"COMPLETE"}
// {"order_id":10161,"order_status":"COMPLETE"}
// {"order_id":10163,"order_status":"COMPLETE"}
// {"order_id":10165,"order_status":"COMPLETE"}
// {"order_id":10170,"order_status":"COMPLETE"}
// {"order_id":10172,"order_status":"COMPLETE"}
// {"order_id":10184,"order_status":"COMPLETE"}
// {"order_id":10185,"order_status":"COMPLETE"}
// {"order_id":10189,"order_status":"COMPLETE"}
// {"order_id":10190,"order_status":"COMPLETE"}


+-------------------+-------------+------+-----+---------+----------------+
| Field             | Type        | Null | Key | Default | Extra          |
+-------------------+-------------+------+-----+---------+----------------+
| order_id          | int(11)     | NO   | PRI | NULL    | auto_increment |
| order_date        | datetime    | NO   |     | NULL    |                |
| order_customer_id | int(11)     | NO   |     | NULL    |                |
| order_status      | varchar(45) | NO   |     | NULL    |                |
+-------------------+-------------+------+-----+---------+----------------+