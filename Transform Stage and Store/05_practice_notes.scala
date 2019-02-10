/**create RDD***/
//start spark spark-shell
//print all conf
//productsRaw variable: use scala source  to get file "/data/retail_db/products/part-00000" lines, convert to list
//productsRDD variable: convert productsRaw to RDD
//take sample and print 100
//read orders as txt file "/public/retail_db/orders"
//read orderItems as txt file "/public/retail_db/order_items"
//read products as txt file "/public/retail_db/products"

spark-shell --master yarn \
	--conf spark.ui.port=12654 \
	--num-executors 1 \
	--executor-memory 512M
sc.getConf.getAll.foreach(println)

var productsRaw=scala.io.Source.fromFile("/data/retail_db/products/part-00000").getLines.toList
var productsRDD=sc.parallelize(productsRaw)
productsRDD.takeSample(true, 100).foreach(println)

var orders=sc.textFile("/public/retail_db/orders")
var orderItems=sc.textFile("/public/retail_db/order_items")
var propucts=sc.textFile("/public/retail_db/products")

orders.take(100).foreach(println)
orderItems.take(100).foreach(println)
propucts.take(100).foreach(println)

/**create RDD***/
//start spark spark-shell
//productsRaw variable: use scala source  to get file "/data/retail_db/products/part-00000" lines, convert to list
//productsRDD variable: convert productsRaw to RDD
//take sample and print 100


/**json***/
//scenarioA
//ordersDF variable:	read from json ("/public/retail_db_json/orders")
//show "order_id","order_date"
//scenarioB
//ordersDF variable:	load from json ("/public/retail_db_json/orders")
//show all data from json

var ordersDF=sqlContext.read.json("/public/retail_db_json/orders")
ordersDF.show
ordersDF.select("order_id", "order_date").show
var ordersDF2=sqlContext.load("/public/retail_db_json/orders","json")
ordersDF2.show


/*****transform stage and store***/
//load orders /public/retail_db/orders
//variable str: get first record
//variable a: split str by comma and check 1,2,3 fields
//convert field 1 to int (field0)
//check field 2 like "2013", check field 2 like "2017"
//variable orderDate: field 2 
//filter for orderDate chars 0-10
//filter for orderDate chars 5-7
//filter for orderDate chars after 11
//replace orderDate - with / in 
//replace orderDate 07 with July
//index place of 2 in orderDate


var orders = sc.textFile("/public/retail_db/orders")
var str = orders.first
var a = str.split(",")
a(0)
a(1)
a(2)
a(0).toInt
a(1).contains("2013")
a(1).contains("2017")

var orderDate=a(1)
orderDate.substring(0,10)
orderDate.substring(5,7)
orderDate.substring(11)
orderDate.replace('-','/')
orderDate.replace("07","July")
orderDate.indexOf("2",2)



/*** map ***/
//load orders /public/retail_db/orders
//variable orderDate and split records by comma, get field 2, chars 0-10, and change - with "" and convert to int
//take 10 records and print
//variable ordersPairedRDD and 
	//split records by comma, tuple field 1 and field 2
	//get field 1 convert to int
	//get field 2, chars 0-10, and change - with "" and convert to int
	//take 10 records and print
/*** flatmap ***/
//variable l: create a list for these 
	//"hello","How are you doing","Let us perform word count","As part of the word count program","we will see how many times each word repeat"
//variable l_rdd: 	convert productsRaw to RDD	
//variable l_flatmap: convert each word to one list
//collect and print l_flatmap
// variable wordcount: count the words from l_flatmap


var orders = sc.textFile("/public/retail_db/orders")
var orderDate = orders.map(o=>o.split(",")(1).substring(0,10).replace("-","").toInt)
orderDate.take(10).foreach(println)
var ordersPairedRDD=orders.map(order=>{
 val o=order.split(",")
 (o(0).toInt,o(1).substring(0,10).replace("-","").toInt)
})
ordersPairedRDD.take(10).foreach(println)

var l=Iterable("hello","How are you doing","Let us perform word count","As part of the word count program","we will see how many times each word repeat").toList
var l_rdd=sc.parallelize(l)
l_rdd.collect.foreach(println)
var l_flatmap=l_rdd.flatMap(rec=>{
 (rec.toLowerCase.split(" ").toList)
})
l_flatmap.collect.foreach(println)
l_flatmap.countByValue

l_flatmap.map(word=>(word,"")).countByKey

/*****aggregations ***/
/*countByKey, reduce, groupByKey, sorting, reduceByKey, aggregateByKey */

/*countByKey*/
//load orders /public/retail_db/orders
//get orders field 3 (order_status) and count by status, print 

var orders = sc.textFile("/public/retail_db/orders")
orders.map(o=>(o.split(",")(3),1)).countByKey.foreach(println)

/*reduce*/
//load orderItems /public/retail_db/order_items
//variable orderItemsRevenue: split rec by comma and get field 5 (subtotal), convert to float
//get the total revenue 

var orderItems=sc.textFile("/public/retail_db/order_items")
var orderItemsRevenue=orderItems.map(order=>{
 order.split(",")(4).toFloat
})
orderItemsRevenue.take(10).foreach(println)
orderItemsRevenue.reduce((total, revenue)=>total+revenue)

/*groupByKey*/
//load orderItems /public/retail_db/order_items
//variable orderItemsMap
	//split records by comma
	//get field 2 (order_id) and field 5 (subtotal) to a tuple
	//convert field 2 to int, field 5 to float
//variable orderItemsGBK and group orderItemsMap
	//print 10 records
//get orderItemsGBK 
	//field 1 and field2 to a tuple
	//convert field 2 to list and summarize
	////print 10 records

var orderItems=sc.textFile("/public/retail_db/order_items")
var orderItemsMap=orderItems.map(orderitem=>{
 var oi=orderitem.split(",")
 (oi(1).toInt, oi(4).toFloat)
})
orderItemsMap.take(10).foreach(println)
var orderItemsGBK=orderItemsMap.groupByKey
orderItemsGBK.take(10).foreach(println)
orderItemsGBK.map(rec=>(rec._1, rec._2.toList.sum)).take(10).foreach(println)

/** sorting */
//variable l iterable "343,5,6343,7,1" convert to list
//use 2 diff way for sorting
//variable ordersSortedByRevenue
	//get all records from orderItemsGBK to a list (flatmap)
	//convert field 2 to list and desc (revenue)
	//map back the orderItemsGBK field 1 and the revenue to a tuple
	//print 10 records

var l = Iterable(343,5,6343,7,1).toList
l.sorted
l.sortBy(o => o)

orderItemsGBK.take(10).foreach(println)

var ordersSortedByRevenue = orderItemsGBK.
 flatMap(rec=>(rec._2.toList.sortBy(o => -o).map(k=>(rec._1,k))))
ordersSortedByRevenue.take(10).foreach(println)

/* reduceByKey */
//load orderItems /public/retail_db/order_items
//variable orderItemsMap
	//split records by comma
	//get field 2 (order_id) and field 5 (subtotal) to a tuple
	//convert field 2 to int, field 5 to float
//variable revenuePerOrderId: get the total revenue per order_id
//variable minRevenuePerOrderId: get the minimum revenue per order_id
//sort minRevenuePerOrderId by order_id

var orderItems=sc.textFile("/public/retail_db/order_items")
var orderItemsMap=orderItems.map(orderitem=>{
 var oi=orderitem.split(",")
 (oi(1).toInt, oi(4).toFloat)
})
var revenuePerOrderId=orderItemsMap.
 reduceByKey((total,rev)=>total+rev)
var minRevenuePerOrderId=orderItemsMap.
 reduceByKey((min,rev)=>if (min<rev) min else rev)
minRevenuePerOrderId.take(10).foreach(println)

minRevenuePerOrderId.sortBy(o => o).take(10).foreach(println)

/* aggregateByKey */
//load orderItems /public/retail_db/order_items
//variable orderItemsMap
	//split records by comma
	//get field 2 (order_id) and field 5 (subtotal) to a tuple
	//convert field 2 to int, field 5 to float
//variable revenueAndMaxPerProductID 
	//*output (order_id, (subtotal))
	//get orderItemsMap and aggregateByKeym result as 0.0,0.0 decimals
	//value1 subtotal: 
		//inter, subtotal => inter field 1 +subtotal
		// if subtotal> inter field 2 then subtotal else inter field 2
	//value2 max: 
		//total, inter => total field 1 + inter field 1
		// if total field 2 > inter field 2 then total field 2 else inter field 2
//print 10 records
//retry the task with 1st decimals










/*****sorting ***/
//task 1
//load products
//split and field 1 (cat_id) and convert to int and we need all the product fields
//sort by category id
//sort desc order
//task 2
//use products and filter field 4<> "" (price not empty)
//and get field 1 as int field 4 (price negative tag) as float, (field1,field4) and all the product fields
//sort by category id
//show the 2 element of the tuple

var products=sc.textFile("/public/retail_db/products")
products.map(rec=>(-rec.split(",")(1).toInt, (rec))).sortByKey().take(10).foreach(println)







/*****ranking ***/
/*sort by price task1 */
//read products txt 
//use products and filter field 4<> "" (price not empty)
//and get field 4 (price) as float, and all the product fields (field4, product fields) 
//sort by desc key 
//print

/*sort by price task2 */
//read products txt 
//use products and filter field 4<> "" (price not empty)
//take ordered by price desc/reverse
//print




/**byKeyRanking ***/
//*ranking -Get top N priced products with in each product category*/
//read products txt 
//filter field4 <>"" and get field1 as float and all the product fields
//group the filtered value
//count products from txt, after filter, and count the grouped value
//print grouped



/**get top N prices for the first category using scala ***/
//read products txt 
//filter field4 <>"" and get field1 as float and all the product fields
//group the filtered value
//get the first records and use the 2nd field and set as productsIterable variable
//get the field4 and convert to float and convert the result to set
//add variable productPrices
//convert productPrices to list, sort, sort desc, print 5 value


/**get top N priced products using scala ***/
//*get all the products in desc order by price*/
//read products txt 
//filter field4 <>"" and get field1 as float and all the product fields
//group the filtered value
//productsIterable variable: 	get the first records and use the 2nd field
//productPrices variable: 		get the field4 and convert to float and convert the result to set
//topNPrices variable:			get productPrices and convert to list and sort desc (field 4) and take 10
//productsSorted variable:		get productsIterable and convert to list and sort desc (field 4) and convert to float
//minOfTopNPrices variable:		get the minimum of topNPrices
//topNPricedProducts variable:	get productsSorted and take field4 while field4 float >= minOfTopNPrices
//now create a def getTopNPricedProducts (productsIterable, topN) to return topNPricedProducts
//test 5 productsIterable, 3 productsIterable


/**get top N products by category ***/
//top3PricedProductsPerCategory variable: 	get productsGroupByCategory and use flat map to sed 3 records (rec 2field =price) to getTopNPricedProducts
//collect all top3PricedProductsPerCategory and print



/*****set operations ***/
//load orders /public/retail_db/orders
//customers_201308 variable: 	get orders, filter field1 if contain "2013-08" and map field2 (customer_id) to int
//customers_201309 variable:	get orders, filter field1 if contain "2013-09" and map field2 (customer_id) to int
//count of unique records for customers_201308
//count of unique records for customers_201309
//get all the customers who placed orders in 2013 Aug and 2013 Sept
//get all the unique customers who placed orders in 2013 Aug or 2013 Sept
//get all the customers who placed orders in 2013 Aug but not in 2013 Sept 
	//map 1 for each customers_201308
	//left join -- map 1 for each customers_201309
	//filter where 2nd of 2nd is none
	//map 1st to get 201308
	//list unique records


/*****saving data with delimiters ***/
//load orders /public/retail_db/orders
//orderCountByStatus variable: 	get orders, map field 1, 1 as num, then countbykey or reducebykey
//save as text file to : "/user/matymar7/order_count_by_status"
//check: read file from "/user/matymar7/order_count_by_status"
//clean hdfs location '/user/matymar7/order_count_by_status"
//get orderCountByStatus and map field1+"\t"+field2 and save as text file to : "/user/matymar7/order_count_by_status"



/***compression ***/
// go to /etc/hadoop.conf and check core-site.xml
//search for codec and copy SnappyCodec
//load orders /public/retail_db/orders
//orderCountByStatus variable: 	get orders, map field 1, 1 as num, then countbykey or reducebykey
//save as text file and use Snappycodec to : "/user/matymar7/ order_count_by_status_snappy"
//check: read file from "/user/matymar7/order_count_by_status_snappy"


/*****save data in different file formats ***/
//parquet file
//variable ordersDF: 	read from json "/public/retail_db_json/orders"
//save ordersDF as parquet to /user/matymar7/orders_parquet 
//load and show /user/matymar7/orders_parquet
//orc file
//variable ordersDF: 	read from json "/public/retail_db_json/orders"
//write to orc format to here /user/matymar7/orders_orc
//load orc and show
//read orc and show