/**Problem statement***/

// Launch Spark shell - understand the environment and use resources optimally


spark-shell --master yarn \
 --num-executors 1 \
 --executor-memory 512M \
 --conf spark.ui.port=12673


// read orders and order_items


val orders = sc.textFile("/public/retail_db/orders")
val orderItems = sc.textFile("/public/retail_db/order_items")

orders.first
orderItem.first

orders.take(10).foreach(println)
orderItems.take(10).foreach(println)

// filter for completed or closed orders

val ordersFiltered = orders.
 filter(order=>order.split(",")(3) == "COMPLETE" || o.split(",")(3) == "CLOSED")

ordersFiltered.take(10).foreach(println)

// convert both filtered orders and order_items to key value pairs

val ordersMap=ordersFiltered.map(o=>(o.split(",")(0).toInt, o.split(",")(1)))
val orderItemsMap = orderItems.map(oi=>(oi.split(",")(1).toInt,(oi.split(",")(2).toInt,oi.split(",")(4).toFloat)))

ordersMap.take(10).foreach(println)

// join the two data sets

val ordersJoin = ordersMap.join(orderItemsMap)
/*output of the join 
(order_id, (order_date, (order_item_product_id, order_item_subtotal)))
*/


// get daily revenue per product id

val ordersJoinMap = ordersJoin.map(rec=> ((rec._2._1, rec._2._2._1), rec._2._2._2))
ordersJoinMap.take(10).foreach(println)
/* ((order_date, order_item_product_id), order_item_subtotal) */

val dailyRevenuePerProductId = ordersJoinMap.
 reduceByKey((revenue, order_item_subtotal)=> revenue+order_item_subtotal)
dailyRevenuePerProductId.take(10).foreach(println)


// load products from local file system and convert into RDD /data/retail_db/products/part-00000



import scala.io.Source
val productsRaw = Source.
 fromFile("/data/retail_db/products/part-00000").
 getLines.
 toList
val products = sc.parallelize(productsRaw)
products.take(10).foreach(println)
products.count




// join daily revenue per product id with products to get daily revenue per product (by name)


val productsMap = products.
 map(product=> (product.split(",")(0).toInt, product.split(",")(2)))

/*((order_date, order_product_id), daily_revenue_per_product_id)*/
val dailyRevenuePerProductIdMap = dailyRevenuePerProductId.
 map(rec=>(rec._1._2,(rec._1._1, rec._2)))
/*(order_product_id, (order_date, daily_revenue_per_product_id))*/

dailyRevenuePerProductIdMap.take(10).foreach(println)

val dailyRevenuePerProductJoin = dailyRevenuePerProductIdMap.join(productsMap)
dailyRevenuePerProductJoin.take(10).foreach(println)



// sort the data by date in ascending order and by daily revenue per product in descending order


val dailyRevenuePerProductSorted = dailyRevenuePerProductJoin.
 map(rec=>((rec._2._1._1, -rec._2._1._2),(rec._2._1._1, rec._2._1._2, rec._2._2))).
 sortByKey()
dailyRevenuePerProductSorted.take(100).foreach(println)



// get data to desired format - order_date, daily_revenue_per_product, product_name


val dailyRevenuePerProduct = dailyRevenuePerProductSorted.
 map(rec=>rec._2._1+","+rec._2._2+","+rec._2._3)
dailyRevenuePerProduct.take(10).foreach(println)



// save final output into HDFS in avro file format as well as text file format
// HDFS location - avro format /user/YOUR_USER_ID/daily_revenue_avro_scala



// HDFS location - text format /user/YOUR_USER_ID/daily_revenue_txt_scala

dailyRevenuePerProduct.saveAsTextFile("/user/matymar7/daily_revenue_txt_scala/")

// Copy both from HDFS to local file system
// /home/YOUR_USER_ID/daily_revenue_scala

mkdir daily_revenue_scala
hadoop fs -get /user/matymar7/daily_revenue_txt_scala \
 /home/matymar7/daily_revenue_scala/daily_revenue_txt_scala

cd daily_revenue_scala/daily_revenue_txt_scala
ls -ltr

