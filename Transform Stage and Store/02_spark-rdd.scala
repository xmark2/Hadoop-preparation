/**create RDD***/
//start spark spark-shell
//productsRaw variable: use scala source  to get file "/data/retail_db/products/part-00000" lines, convert to list
//productsRDD variable: convert productsRaw to RDD
//take sample and print 100

spark-shell --master yarn \
	--conf spark.ui.port=12654 \
	--num-executors 1 \
	--executor-memory 512M
sc.getConf.getAll.foreach(println)


val orders = sc.textFile("/public/retail_db/orders")
orders.first
orders.take(10)

val productsRaw = scala.io.Source.fromFile("/data/retail_db/products/part-00000").getLines.toList

val productsRDD = sc.parallelize(productsRaw)

productsRDD.take(10)

orders.count

orders.takeSample(true,100)
orders.takeSample(true,100).foreach(println)

orders.collect

/**json***/
//scenarioA
//ordersDF variable:	read from json ("/public/retail_db_json/orders")
//show "order_id","order_date"
//scenarioB
//ordersDF variable:	load from json ("/public/retail_db_json/orders")
//show all data from json

/*scenarioA to read json***/

val ordersDF=sqlContext.read.json("/public/retail_db_json/orders")
ordersDF.show
ordersDF.schema
ordersDF.select("order_id","order_date")
ordersDF.select("orde_id","order_date").show

/*scenarioB to read json*/
val ordersDF=sqlContext.load("/public/retail_db/orders","json")
ordersDF.show





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


val orders=sc.textFile("/public/retail_db/orders")
val str = orders.first
val a = str.split(",")
a(0)
a(1)
a(2)

val orderid = a(0)
val orderid = a(0).toInt

a(1)
a(1).contains("2017")
a(1).contains("2013")

val orderDate=a(1)
orderDate.substring(0,10)
orderDate.substring(5,7)
orderDate.substring(11)


orderDate.replace('-','/')
orderDate.replace("07","July")

orderDate.indexOf("2")
orderDate.indexOf("2", 2)


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




/*** map ***/

val orders=sc.textFile("/public/retail_db/orders")
val orderDates = orders.map((str: String) => {
 str.split(",")(1).substring(0,10).replace("-","").toInt 
})
orderDates.take(10).foreach(println)



val ordersPairedRDD=orders.map(order=>{
 val o = order.split(",")
 (o(0)).toInt, o(1).substring(0,10).replace("-","").toInt)
})
ordersPairedRDD.take(10).foreach(println)


val orderItems=sc.textFile("/public/retail_db/order_items")
val orderItemsPairedRDD=orderItems.map(orderItem =>{
	(orderItems.split(",")(1).toInt, orderItem)
})

orderItems.take(10).foreach(println)



/*** flatmap ***/

val l = List("hello","How are you doing","Let us perform word count","As part of the word count program","we will see how many times each word repeat")

val l_rdd=sc.parallelize(l)
val l_map=l_rdd.map(ele => ele.split(" "))
val l_flatMap=l_rdd.flatMap(ele => ele.split(" "))

l_map.collect.foreach(println)
l_flatMap.collect.foreach(println)

val wordcount = l_flatMap.map(word => (word, "")).countByKey






/*** filters ***/

val orders=sc.textFile("/public/retail_db/orders")

orders.filter(order => order.split(",")(3) == "COMPLETE")
orders.filter(order => order.split(",")(3) == "COMPLETE").take(10).foreach(println)

orders.filter(order => order.split(",")(3) == "COMPLETE").count

val s = orders.first
s.contains("COMPLETE") || s.contains("CLOSED")

s.split(",")(3).contains("COMPLETE") || s.split(",")(3).contains("CLOSED")

(s.split(",")(3)=="COMPLETE" || s.split(",")(3)=="CLOSED") && (s.split(",")(1).contains("2013-07-25"))
(s.split(",")(3)=="COMPLETE" || s.split(",")(3)=="CLOSED") && (s.split(",")(1).contains("2013-08-25"))

orders.filter(order => order.split(",")(3)=="COMPLETE")
orders.filter(order => order.split(",")(3)=="COMPLETE").count

orders.map(order => order.split(",")(3)).distinct
orders.map(order => order.split(",")(3)).distinct.collect.foreach(println)


val ordersFiltered = orders.filter(order => {
	val o = order.split(",")
	(o(3) == "COMPLETE" || o(3) == "CLOSED") && (o(1).contains("2013-09"))
})

ordersFiltered.take(10).foreach(println)
ordersFiltered.count








/*** **  joins ***/


/*** inner join ***/

val orders=sc.textFile("/public/retail_db/orders")
val orderitems=sc.textFile("/public/retail_db/order_items")


val ordersMap=orders.map(order=> {
 (order.split(",")(0).toInt,order.split(",")(1).substring(0,10))
})
ordersMap.take(10).foreach(println)
orders.count
ordersMap.count


val orderItemsMap=orderItems.map(orderItem=> {
 val oi=orderItem.split(",")
 (oi(1).toInt,oi(4).toFloat)
})
orderItemsMap.take(10).foreach(println)
orderItemsMap.count
orderItemsMap.count

val ordersJoin = ordersMap.join(orderItemsMap)
ordersJoin.take(10).foreach(println)
ordersJoin.count



/*** outer join ***/

val ordersMap=orders.map(order=> {
 (order.split(",")(0).toInt,order)
})

val orderItemsMap=orderItems.map(orderItem=> {
 val oi=orderItem.split(",")
 (oi(1).toInt,orderItem)
})

val ordersLeftOuterJoin = ordersMap.leftOuterJoin(orderItemsMap)


val t = ordersLeftOuterJoin.first
t._2._2

val ordersLeftOuterJoinFilter = ordersLeftOuterJoin.filter(order=>order._2._2==None)
ordersLeftOuterJoinFilter.take(10).foreach(println)

val ordersWithNoOrderItem = ordersLeftOuterJoinFilter.map(order=>order._2._1)
ordersWithNoOrderItem.take(10).foreach(println)


val ordersRightOuterJoin = orderItemsMap.rightOuterJoin(ordersMap)
val ordersWithNoOrderItem = ordersRightOuterJoin.
 filter(order=>order._2._1==None).
 map(order=>order._2._2)
ordersWithNoOrderItem.take(10).foreach(println)






/*****aggregations ***/
/*countByKey, reduce, groupByKey, sorting, reduceByKey, aggregateByKey */

/*countByKey*/
//load orders /public/retail_db/orders
//get orders field 3 (order_status) and count by status, print 

/*reduce*/
//load orderItems /public/retail_db/order_items
//variable orderItemsRevenue: split rec by comma and get field 5 (subtotal), convert to float
//get the total revenue 

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



/*countByKey*/
val orders=sc.textFile("/public/retail_db/orders")

orders.map(order=>(order.split(",")(3),"")).countByKey.foreach(println)




/*reduce*/
val orderItems=sc.textFile("/public/retail_db/order_items/")
orderItems.take(10).foreach(println)

val orderItemsRevenue = orderItems.map(oi=> oi.split(",")(4).toFloat)
orderItemsRevenue.take(10).foreach(println)

orderItemsRevenue.reduce((total, revenue) => total+revenue)
val orderItemsMaxRevenue=orderItemsRevenue.reduce((max,revenue) => {
 if(max<revenue) revenue else max
})






/*groupByKey*/

val orderItemsMap = orderItems.map(oi => (oi.split(",")(1).toInt, oi.split(",")(4).toFloat))
orderItemsMap .take(10).foreach(println)  


val orderItemsGBK=orderItemsMap.groupByKey
orderItemsGBK.take(10).foreach(println)    

orderItemsGBK.map(rec=>rec._2.toList.sum).take(10).foreach(println)
orderItemsGBK.map(rec=>(rec._1, rec._2.toList.sum)).take(10).foreach(println)  









/** sorting */
//variable l iterable "343,5,6343,7,1" convert to list
//use 2 diff way for sorting
//variable ordersSortedByRevenue
	//get all records from orderItemsGBK to a list (flatmap)
	//convert field 2 to list and desc (revenue)
	//map back the orderItemsGBK field 1 and the revenue to a tuple
	//print 10 records

val l = Iterable(343,5,6343,7,1).toList

l.sorted
l.sortBy(o=>-o)

val ordersSortedByRevenue = orderItemsGBK.
 flatMap(rec=>{
 rec._2.toList.sortBy(o => -o).map(k=>(rec._1,k))
})

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


val orderItems=sc.textFile("/public/retail_db/order_items/") 
val orderItemsMap = orderItems.map(oi => (oi.split(",")(1).toInt, oi.split(",")(4).toFloat))

val revenuePerOrderId=orderItemsMap.
 reduceByKey((total, revenue)=>total+revenue)

val minRevenuePerOrderId=orderItemsMap.
 reduceByKey((min, revenue)=>if(min>revenue) revenue else min)

revenuePerOrderId.take(10).foreach(println)
minRevenuePerOrderId.take(10).foreach(println)

orderItemsMap.sortByKey().take(10).foreach(println)

minRevenuePerOrderId.sortByKey().take(10).foreach(println)





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



val orderItems=sc.textFile("/public/retail_db/order_items/") 
val orderItemsMap = orderItems.map(oi => (oi.split(",")(1).toInt, oi.split(",")(4).toFloat))

val revenueAndMaxPerProductID=orderItemsMap.
 aggregateByKey((0.0, 0.0))(
 (inter, subtotal)=>(inter._1+subtotal, if(subtotal>inter._2) subtotal else inter._2),
 (total, inter)=>(total._1+inter._1, if(total._2>inter._2) total._2 else inter._2)
)
revenueAndMaxPerProductID.take(10).foreach(println)

revenueAndMaxPerProductID.sortByKey().take(10).foreach(println)

//(order_id, order_item_subtotal)
val revenueAndMaxPerProductID=orderItemsMap.
 aggregateByKey((0.0f, 0.0f))(
 (inter, subtotal)=>(inter._1+subtotal, if(subtotal>inter._2) subtotal else inter._2),
 (total, inter)=>(total._1+inter._1, if(total._2>inter._2) total._2 else inter._2)
)
//(order_id, (order_revenue, max_order_item_subtotal))









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


//task 1
val products=sc.textFile("/public/retail_db/products/")
products.take(10).foreach(println)

val productsMap=products.map(product=>(product.split(",")(1).toInt,product))

productsMap.take(10).foreach(println)

val productsSortedByCategoryId = productsMap.sortByKey()

productsSortedByCategoryId.take(10).foreach(println)

/*desc order */
val productsSortedByCategoryId = productsMap.sortByKey(false)
productsSortedByCategoryId.take(10).foreach(println)

//task 2
val productsMap = products.
 filter(product=>product.split(",")(4)!="").
 map(product=>((product.split(",")(1).toInt,-product.split(",")(4).toFloat),product))

val productsSortedByCategoryId=productsMap.sortByKey()
productsSortedByCategoryId.take(10).foreach(println)


val productsSortedByCategoryId=productsMap.sortByKey().map(rec=>rec._2)
productsSortedByCategoryId.take(10).foreach(println)




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

/*sort by price task1 */
val products=sc.textFile("/public/retail_db/products")
val productsMap=products.
 filter(product=>product.split(",")(4)!="").
 map(product=>(product.split(",")(4).toFloat,product))
val productsSortedByPrice=productsMap.sortedByKey(false)
productsSortedByPrice.take(10).foreach(println)

/*sort by price task2 */
products.
 filter(product=>product.split(",")(4)!="").
 takeOrdered(10)(Ordering[Float].reverse.on(product=>product.split(",")(4).toFloat)).
 foreach(println)


/**byKeyRanking ***/
//*ranking -Get top N priced products with in each product category*/
//read products txt 
//filter field4 <>"" and get field1 as float and all the product fields
//group the filtered value
//count products from txt, after filter, and count the grouped value
//print grouped

val products=sc.textFile("/public/retail_db/products")
val productsMap=products.
 filter(product=>product.split(",")(4)!="").
 map(product=>(product.split(",")(1).toFloat,product))

productsMap.take(10).foreach(println)

val productsGroupByCategory = productsMap.groupByKey

products.count

productsMap.count

productsGroupByCategory.count

productsGroupByCategory.take(10).foreach(println)



/**get top N prices for the first category using scala ***/
//read products txt 
//filter field4 <>"" and get field1 as float and all the product fields
//group the filtered value
//get the first records and use the 2nd field and set as productsIterable variable
//get the field4 and convert to float and convert the result to set
//add variable productPrices
//convert productPrices to list, sort, sort desc, print 5 value

products.first
productsGroupByCategory.first

val productsIterable = productsGroupByCategory.first._2

productsIterable.take(10).foreach(println)

productsIterable.map(p=>p.split(",")(4).toFloat)
productsIterable.map(p=>p.split(",")(4).toFloat).toSet

val productPrices = productsIterable.map(p=>p.split(",")(4).toFloat).toSet

productPrices.toList
productPrices.toList.sortBy
productPrices.toList.sortBy(p=>-p)
productPrices.toList.sortBy(p=>-p).take(5)




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

val productsSorted = productsIterable.toList.sortBy(product=>-product.split(",")(4).toFloat)
productsSorted.foreach(println)


//val minOfTopNPrices = topNPrices.min

val topNPricedProducts = productsSorted.takeWhile(product=>product.split(",")(4).toFloat >= minOfTopNPrices)
topNPricedProducts.foreach(println)


def getTopNPricedProducts(productsIterable: Iterable[String], topN: Int): Iterable[String] = {
 val productPrices = productsIterable.map(p=>p.split(",")(4).toFloat).toSet
 val topNPrices = productPrices.toList.sortBy(p => -p).take(topN)

 val productsSorted = productsIterable.toList.sortBy(product => -product.split(",")(4).toFloat)
 val minOfTopNPrices = topNPrices.min

 val topNPricedProducts = productsSorted.takeWhile(product => product.split(",")(4).toFloat >= minOfTopNPrices)
 topNPricedProducts
}

getTopNPricedProducts(productsIterable, 5)
getTopNPricedProducts(productsIterable, 5).foreach(println)
getTopNPricedProducts(productsIterable, 3).foreach(println)




/**get top N products by category ***/
//top3PricedProductsPerCategory variable: 	get productsGroupByCategory and use flat map to sed 3 records (rec 2field =price) to getTopNPricedProducts
//collect all top3PricedProductsPerCategory and print


productsGroupByCategory.take(10).foreach(println)

val top3PricedProductsPerCategory = productsGroupByCategory.flatMap(rec=>getTopNPricedProducts(rec._2,3))
top3PricedProductsPerCategory.collect.foreach(println)





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


val orders=sc.textFile("/public/retail_db/orders")

orders.first
val customers_201308=orders.
 filter(order=>order.split(",")(1).contains("2013-08")).
 map(order=>order.split(",")(2).toInt)

customers_201308.count
customers_201308.distinct.count

val customers_201309=orders.
 filter(order=>order.split(",")(1).contains("2013-09")).
 map(order=>order.split(",")(2).toInt)

customers_201309.distinct.count  

//get all the customers who placed orders in 2013 Aug and 2013 Sept
val customers_201308_and_201309=customers_201308.intersection(customers_201309)
customers_201308_and_201309.distinct.count

//get all the unique customers who placed orders in 2013 Aug or 2013 Sept

val customers_201308_union_201309 = customers_201308.union(customers_201309)
customers_201308_union_201309.count
customers_201308_union_201309.distinct.count  

val customers_201308_union_201309 = customers_201308.union(customers_201309).distinct

//get all the customers who placed orders in 2013 Aug but not in 2013 Sept    

val customer_201308_minus_201309 = customers_201308.
 leftOuterJoin(customers_201309)

customers_201308.map(c=>(c,l)).
 leftOuterJoin(customers_201309.map(c=>(c,l))).
 take(100).
 foreach(println)



customer_201308_minus_201309.distinct.count

 customer_201308_minus_201309.distinct.take(10).foreach(println)

//get all the customers who placed orders in 2013 Aug but not in 2013 Sept  
val customer_201308_minus_201309 = customers_201308.map(c=>(c,1)).
 leftOuterJoin(customers_201309.map(c=>(c,1))).
 filter(rec=>rec._2._2 == None).
 map(rec=>rec._1).
 distinct


/*****saving data with delimiters ***/
//load orders /public/retail_db/orders
//orderCountByStatus variable: 	get orders, map field 1, 1 as num, then countbykey or reducebykey
//save as text file to : "/user/matymar7/order_count_by_status"
//check: read file from "/user/matymar7/order_count_by_status"
//clean hdfs location '/user/matymar7/order_count_by_status"
//get orderCountByStatus and map field1+"\t"+field2 and save as text file to : "/user/matymar7/order_count_by_status"

val orders=sc.textFile("/public/retail_db/orders")

val orderCountByStatus = orders.
 map(order=>(order.split(",")(3),1)).
 reduceByKey((total, element)=>total+element)

orderCountByStatus.saveAsTextFile

orderCountByStatus.saveAsTextFile("/user/matymar7/order_count_by_status")

sc.saveAsTextFile("/user/matymar7/order_count_by_status").collect.foreach(println)

orderCountByStatus.
 map(rec=>rec._1+"\t"+rec._2).
 saveAsTextFile("/user/matymar7/order_count_by_status")

//let's delete the data if we have anything in target folder '

hdfs dfs -rm -r /user/matymar7/order_count_by_status


orderCountByStatus.
 map(rec=>rec._1+"\t"+rec._2).
 saveAsTextFile("/user/matymar7/order_count_by_status")

sc.saveAsTextFile("/user/matymar7/order_count_by_status").collect.foreach(println) 

//let's check the file in hdfs
hdfs dfs -ls /user/matymar7/order_count_by_status/part*


/***compression ***/
// go to /etc/hadoop.conf and check core-site.xml
//search for codec and copy SnappyCodec
//load orders /public/retail_db/orders
//orderCountByStatus variable: 	get orders, map field 1, 1 as num, then countbykey or reducebykey
//save as text file and use Snappycodec to : "/user/matymar7/ order_count_by_status_snappy"
//check: read file from "/user/matymar7/order_count_by_status_snappy"

cd /etc/hadoop.conf
vi core-site.xml
/codec

copy org.apache.hadoop.io.compress.SnappyCodec from conf file

hdfs dfs -ls /user/matymar7/order_count_by_status_snappy

sc.textFile("/user/matymar7/order_count_by_status_snappy").collect.foreach(println)

//using compression
orderCountByStatus.
saveAsTextFile("/user/matymar7/ order_count_by_status_snappy",
classOf[org.apache.hadoop.io.compress.SnappyCodec])

hdfs dfs -ls /user/matymar7/order_count_by_status_snappy

 sc.textFile("/user/matymar7/order_count_by_status_snappy").collect.foreach(println)


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

val ordersDF=sqlContext.read.json("/public/retail_db_json/orders")

ordersDF.save("/user/matymar7/orders_parquet", "parquet")

sqlContext.load 

sqlContext.load("/user/matymar7/orders_parquet", "parquet").show

//writing data into different file formats

var ordersDF = sqlContext.read.json("/public/retail_db_json/orders")
ordersDF.save("/user/matymar7/orders_parquet", "parquet")
ordersDF.write.orc("/user/matymar7/orders_orc")

sqlContext.load("/user/matymar7/orders_orc","orc")
sqlContext.load("/user/matymar7/orders_orc","orc") .show

sqlContext.read.orc("/user/matymar7/orders_orc").show 
 
