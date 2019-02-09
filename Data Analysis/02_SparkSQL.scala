/***Create Data Frame and Register as Temp table*/


sc
//org.apache.spark.SparkContext = ...

sqlContext
//org.apache.spark.SQLContext = ...

val ordersRDD = sc.textFile("/public/retail_db/orders")

ordersRDD.take(10).foreach(println)

val ordersDF = ordersRDD.map(order => { 
 (order.split(",")(0).toInt, order.split(",")(1), order.split(",")(2).toInt, order.split(",")(3))
 }).toDF

ordersDF.show()

//now let's name our fields//


val ordersDF = ordersRDD.map(order => { 
 (order.split(",")(0).toInt, order.split(",")(1), order.split(",")(2).toInt, order.split(",")(3))
 }).toDF("order_id","order_date","order_customer_id","order_status")

ordersDF.show()

ordersDF.printSchema

ordersDF.registerTempTable("orders")
sqlContext.sql("select order_status, count(1) from orders group by order_status").show()

val productsRaw = scala.io.Source.fromFile("/data/retail_db/products/part-00000").getLines
productsRaw.take(10).foreach(println)


val productsRaw = scala.io.Source.fromFile("/data/retail_db/products/part-00000").getLines.toList
val productsRDD = sc.parallelize(productsRaw)

val productsDF = productsRDD.map(product => {
 (product.split(",")(0).toInt, product.split(",")(2))
}).
toDF("product_id", "product_name")

productsDF.show()


productsDF.registerTempTable("products")
sqlContext.sql("select * from products").show



/***Writing Spark SQL Applications*/
/******Process Data*/

spark-shell --master yarn --conf spark.ui.port=12345

//Connect to any databases where we store retail_db in this example we chose orc one
sqlContext.sql("use matymar7_retail_db_orc")

//create dataframe
val productsRaw = scala.io.Source.fromFile("/data/retail_db/products/part-00000").getLines.toList
val productsRDD = sc.parallelize(productsRaw)

val productsDF = productsRDD.map(product => {
 (product.split(",")(0).toInt, product.split(",")(2))
}).
toDF("product_id", "product_name")

//Register temp table
productsDF.registerTempTable("products")
sqlContext.sql("show tables").show

//Let's run our query
sqlContext.sql("SELECT o.order_date, p.product_name, sum(oi.order_item_subtotal) daily_revenue_per_product " +
"FROM orders o JOIN order_items oi " +
"ON o.order_id = oi.order_item_order_id " +
"JOIN products p ON p.product_id = oi.order_item_product_id " +
"WHERE o.order_status IN ('COMPLETE', 'CLOSED') "+
"GROUP BY o.order_date, p.product_name "+
"ORDER BY o.order_date, daily_revenue_per_product").
show

//You can notice the previous query using around 200 task, that is overkill
//Change the query to desc and reduce the tasks

sqlContext.setConf("spark.sql.shuffle.partitions","2")
sqlContext.sql("SELECT o.order_date, p.product_name, sum(oi.order_item_subtotal) daily_revenue_per_product " +
"FROM orders o JOIN order_items oi " +
"ON o.order_id = oi.order_item_order_id " +
"JOIN products p ON p.product_id = oi.order_item_product_id " +
"WHERE o.order_status IN ('COMPLETE', 'CLOSED') "+
"GROUP BY o.order_date, p.product_name "+
"ORDER BY o.order_date, daily_revenue_per_product desc").
show


/******Save data into Hive tables*/

sqlContext.sql("CREATE DATABASE matymar7_daily_revenue")
sqlContext.sql("CREATE TABLE matymar7_daily_revenue.daily_revenue "+
"(order_date string, product_name string, daily_revenue_per_product float) "+
"STORED AS orc")

val daily_revenue_per_product = sqlContext.sql("SELECT o.order_date, p.product_name, sum(oi.order_item_subtotal) daily_revenue_per_product " +
"FROM orders o JOIN order_items oi " +
"ON o.order_id = oi.order_item_order_id " +
"JOIN products p ON p.product_id = oi.order_item_product_id " +
"WHERE o.order_status IN ('COMPLETE', 'CLOSED') "+
"GROUP BY o.order_date, p.product_name "+
"ORDER BY o.order_date, daily_revenue_per_product desc")

daily_revenue_per_product.insertInto("matymar7_daily_revenue.daily_revenue")

sqlContext.sql("select * from matymar7_daily_revenue.daily_revenue").show
daily_revenue_per_product.insertInto

//check the function with tab 
daily_revenue_per_product.
daily_revenue_per_product.saveAsTable
daily_revenue_per_product.write.


/******Spark SQL - DataFrame Operations*/
/*
	show
	select
	filter
	join
	and more...
*/

//check the function with tab 
daily_revenue_per_product.

daily_revenue_per_product.show
daily_revenue_per_product.show(100)

//check the function with tab 
daily_revenue_per_product.save


daily_revenue_per_product.save("/user/matymar7/daily_revenue_save", "json")

hadoop fs -ls /user/matymar7/daily_revenue_save

hadoop fs -tail /user/matymar7/daily_revenue_save/part-r-00000-...

//now check the write function with tab 
daily_revenue_per_product.write.

daily_revenue_per_product.write.json("/user/matymar7/daily_revenue_write")

hadoop fs -ls /user/matymar7/daily_revenue_write

hadoop fs -tail /user/matymar7/daily_revenue_write/part-r-00000-...



/******RDD format*/

daily_revenue_per_product.rdd.take(10).foreach(println)

daily_revenue_per_product.select("order_date")
daily_revenue_per_product.select("order_date").show


//Multiple fields
daily_revenue_per_product.select("order_date", "daily_revenue_per_product").show


daily_revenue_per_product.select("order_date", "daily_revenue_per_product").count

daily_revenue_per_product.filter(daily_revenue_per_product("order_date") === "2013-07-25 00:00:00.0")

daily_revenue_per_product.filter(daily_revenue_per_product("order_date") === "2013-07-25 00:00:00.0").show
daily_revenue_per_product.filter(daily_revenue_per_product("order_date") === "2013-07-25 00:00:00.0").count


//summary notes about save-write-select-filter RDD
daily_revenue_per_product.save("/user/matymar7/daily_revenue_save", "json")
daily_revenue_per_product.write.json("/user/matymar7/daily_revenue_write")
daily_revenue_per_product.select("order_date", "daily_revenue_per_product")
daily_revenue_per_product.filter(daily_revenue_per_product("order_date") === "2013-07-25 00:00:00.0").count





