========
problem1
========

mkdir itversity
cd itversity

sqoop eval \
--connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" \
--username "retail_dba" \
--password "cloudera" \
--query "select * from orders"

sqoop import \
--connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" \
--username "retail_dba" \
--password "cloudera" \
--table "orders" \
--target-dir "/user/cloudera/itversity/problem1/solution" \
--fields-terminated-by "," \
--as-textfile



========
problem2
========

[cloudera@quickstart ~]$ cd data
[cloudera@quickstart data]$ cd retail_db
[cloudera@quickstart retail_db]$ pwd
/home/cloudera/data/retail_db


spark-shell --master yarn \
--conf spark.ui.port=12654 \
--num-executors 1 \
--executor-memory 512M


hdfs dfs -copyFromLocal -f retail_db


import scala.io.Source

var ordersRaw = Source.fromFile("/home/cloudera/data/retail_db/orders/part-00000").getLines.toList
var orders = sc.parallelize(ordersRaw)


var customersRaw = Source.fromFile("/home/cloudera/data/retail_db/customers/part-00000").getLines.toList
var customers = sc.parallelize(customersRaw)

ordersRaw.map(_.split(',')).map(rec=>(rec(0),rec(1))).foreach(println)
orders.map(rec=>(rec(0),rec(1),rec(2),rec(3))).take(10).foreach(println)


var ordersDF = orders.
map(rec=>rec.split(',')).
map(rec=>(rec(0),rec(1),rec(2),rec(3))).
toDF("order_id", "order_date", "order_customer_id", "order_status")

var customersDF = customers.
map(rec=>rec.split(',')).
map(rec=>(rec(0),rec(1),rec(2))).
toDF("customer_id", "customer_fname", "customer_lname")




ordersDF.registerTempTable("orders")

customersDF.registerTempTable("customers")

var result = sqlContext.sql({"""select 
c.customer_lname, c.customer_fname
from customers c 
left outer join orders o 
on c.customer_id=o.order_customer_id
where order_id is null
order by c.customer_lname, c.customer_fname"""}).

result.repartition(1).map(rec=>(rec(0)+","+rec(1))).saveAsTextFile("/user/cloudera/itversity/problem2/solution")
// .show()




========
problem3
========


spark-shell --master yarn \
--conf spark.ui.port=12654 \
--num-executors 1 \
--executor-memory 512M


// Data is available in HDFS under public/crime/csv

var crimeData = sc.textFile("public/crime/csv/rows.csv")


var crimeHeader = crimeData.first

var crimeDataWithoutHeader = crimeData.filter(rec=>rec!=crimeHeader)

crimeDataWithoutHeader.take(10).foreach(println)

crimeDataWithoutHeader.map(rec=> rec.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)).first

// Primary Type, Location Description

var crimeDF = crimeDataWithoutHeader.map(rec=> {
val t = rec.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)
(t(5),t(7))
}).toDF("type","description")


crimeDF.registerTempTable("crime")


var result = sqlContext.sql({"""
select type as crime_type, 
count(1) as incident_count 
from crime 
where description='RESIDENCE'
group by type 
limit 3"""})


result.toJSON.saveAsTextFile("/user/cloudera/itversity/problem3/solution")



========
problem4
========


import scala.io.Source

var nyseRaw = Source.fromFile("data/nyse/NYSE_2017.txt").getLines.toList

var nyseData = sc.parallelize(nyseRaw)

// nyseData.take(10).foreach(println)

var result = nyseData.map(rec=>rec.split(',')).
map(rec=>(rec(0),rec(1),rec(2),rec(3),rec(4),rec(5),rec(6))).
toDF("stockticker","transactiondate","openprice","highprice","lowprice","closeprice","volume")

result.write.parquet("/user/cloudera/itversity/problem4/solution")


========
problem5
========

var data = sc.
sequenceFile("public/randomtextwriter",classOf[org.apache.hadoop.io.Text],classOf[org.apache.hadoop.io.Text])


var seqData = data.map(x=>x._2.toString)

var words = seqData.flatMap(rec=>rec.split(" "))

var wordcount = words.map(word=>(word,1)).reduceByKey(_+_).toDF("word","count")

wordcount.take(10).foreach(println)

import com.databricks.spark.avro._;

wordcount.write.avro("/user/cloudera/itversity/problem5/solution")

var data = sc.textFile("public/randomtextwriter")

========
problem6
========

var orders = sc.textFile("public/retail_db/orders")

var customers = sc.textFile("public/retail_db/customers")


var ordersDF = orders.
map(rec=>rec.split(",")).
map(rec=>(rec(0),rec(1),rec(2),rec(3))).
toDF("order_id","order_date","order_customer_id","order_status")

var customersDF = customers.
map(rec=>rec.split(",")).
map(rec=>(rec(0),rec(1),rec(2),rec(3))).
toDF("customer_id","customer_fname","customer_lname","customer_state") 



ordersDF.registerTempTable("orders")

customersDF.registerTempTable("customers")

var result = sqlContext.sql({"""
select c.customer_fname, 
c.customer_lname, 
count(o.order_id) as order_count
from orders o 
inner join customers c 
on o.order_customer_id=c.customer_id
where customer_state='TX'
group by c.customer_fname,c.customer_lname"""})


result.map(rec=>rec.mkString("\t")).saveAsTextFile("/user/cloudera/itversity/problem6/solution")
