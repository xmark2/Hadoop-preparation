/**Problem statement***/

// Launch Spark shell - understand the environment and use resources optimally
// read orders and order_items
// filter for completed or closed orders
// convert both filtered orders and order_items to key value pairs
// join the two data sets
// get daily revenue per product id
// load products from local file system and convert into RDD /data/retail_db/products/part-00000
// join daily revenue per product id with products to get daily revenue per product (by name)
// sort the data by date in ascending order and by daily revenue per product in descending order
// get data to desired format - order_date, daily_revenue_per_product, product_name
// save final output into HDFS in avro file format as well as text file format
// HDFS location - avro format /user/YOUR_USER_ID/daily_revenue_avro_scala
// HDFS location - text format /user/YOUR_USER_ID/daily_revenue_txt_scala
// Copy both from HDFS to local file system
// /home/YOUR_USER_ID/daily_revenue_scala