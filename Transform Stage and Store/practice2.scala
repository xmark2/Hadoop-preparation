spark-shell --master yarn \
--conf spark.ui.port=12654 \
--num-executors 1 \
--executor-memory 512M


val name=sc.textFile("spark5/EmpName.csv")
val salary=sc.textFile("spark5/EmpSalary.csv")

val namepairRDD=name.map(rec=>(rec.split(",")(0), rec.split(",")(1)))

val salarypairRDD=salary.map(rec=>(rec.split(",")(0), rec.split(",")(1)))

val joined



val grpByKey= swapped.groupByKey().collect


val rddBykey=grpByKey.map{case(k,v)=>k->sc.makeRDD(v.toSeq)}

rddBykey.foreach{case(k,rdd)=>rdd.saveAsTextFile("emp"+k)}



spark-shell --master yarn \
--conf spark.ui.port=12654 \
--num-executors 1 \
--executor-memory 512M


val content = sc.textFile("spark3/sparkdir1/file1.txt,spark3/sparkdir2/file2.txt,spark3/sparkdir3/file3.txt")


content.first
content.take(10).foreach(println)
content.collect.foreach(println)


val flatContent = content.flatMap(rec => rec.split(" "))

val trimmedContent = flatContent.map(rec => rec.trim)

val removeRDD = sc.parallelize(List("a","the","an","as","a","with","this","these","is","are","in","for","to","and","the","of"))

val filtered =trimmedContent.subtract(removeRDD)

val pairedRDD = filtered.map(word => (word,1))

val wordCount = pairedRDD.reduceByKey(_+_)

swapped.take(10).foreach(println)

val swapped = wordCount.map(rec => rec.swap)

val sortedOutput = swapped.sortByKey(false)

sortedOutput.saveAsTextFile("spark3/result")


import org.apache.hadoop.io.compress.Gzipcodec
sortedOutput.saveAsTextFile("spark3/compressedresult",classOf[Gzipcodec])


sortedOutput.saveAsTextFile("spark3/compressedresultGzip",classOf[org.apache.hadoop.io.compress.GzipCodec])


sortedOutput.saveAsTextFile("spark3/compressedresultSnappy",classOf[org.apache.hadoop.io.compress.SnappyCodec])


spark-shell --master yarn \
--conf spark.ui.port=12654 \
--num-executors 1 \
--executor-memory 512M


val name = sc.textFile("spark5/EmployeeName.csv")
val salary = sc.textFile("spark5/Employeesalary.csv")

val namepairRDD = name.map(rec=>(rec.split(",")(0),rec.split(",")(1)))
val salarypairRDD = salary.map(rec=>(rec.split(",")(0),rec.split(",")(1)))

val joined = namepairRDD.join(salarypairRDD)

val keyRemoved = joined.values

keyRemoved.collect.foreach(println)


val swapped = keyRemoved.map(rec=>rec.swap)


val grpByKey = swapped.groupByKey().collect()

val rddByKey = grpByKey.map{case(k,v)=>k->sc.makeRDD(v.toSeq)}

rddByKey.foreach{case(k,rdd)=>rdd.saveAsTextFile("spark5/Emplyee"+k.trim)}

val swapped = keyRemoved.map(rec=>rec.swap)





val headers = headerAndrow.first

val data = headerAndrow.filter(_(0)!=headers(0))

val maps = data.map(splits=>splits.zip(headers).toMap)

val result = maps.filter(map=>map("id")"myself")


result.saveAsTextFile("spark6/result.txt")



val csv = sc.textFile("spark6/user.csv")

val headerAndRows = csv.map(rec=>rec.split(",").map(_.trim))

data.take(10).foreach(println)


val headers = headerAndRows.first

val data = headerAndRows.filter(_(0)!=headers(0))


val maps = data.map(splits=>headers.zip(splits).toMap)


val result = maps.filter(map=>map("id")!="myself")

result.take(10).foreach(println)

maps["id"].first

result.saveAsTextFile("spark6/result.txt")


spark-shell --master yarn \
--conf spark.ui.port=12654 \
--num-executors 1 \
--executor-memory 512M

val name = sc.textFile("spark8/data.csv")

name.sortByKey().take(100).foreach(println)

sortByKey

val namepairRDD = name.map(rec=>(rec.split(",")(0),rec.split(",")(1)))

val swapped = namepairRDD.map(rec=>rec.swap)

swapped.take(10).foreach(println)

val combinedOutput = namepairRDD.combineByKey(List(_),(x:List[String], y:String)=>y:x,(x:List[String], y:List[String])=>x:y)


spark-shell --master yarn \
--conf spark.ui.port=12654 \
--num-executors 1 \
--executor-memory 512M








val feedbackRDD = sc.textFile("spark9/feedback.txt")

feedbackRDD.collect.foreach(println)


val reg1 = """(\d+)\s(\w{3}+)(,)\s(\d{4})""".r //11 Jan, 2015
val reg2 = """(\d+)(\/)(\d+)(\/)(\d{4})""".r //6/17/2014|5
val reg3 = """(\d+)(-)(\d+)(-)(\d{4})""".r //22-08-2013|5
val reg4 = """(\w{3})\s(\d+)\s(\d{4})""".r//Jan 11, 2015|5


val reg1 = """(\d+)\s(\w{3})(,)\s(\d{4})""".r //11 Jan,2015

val reg2 = """(\d+)(/)(\d+)(/)(\d{4})""".r //6/17/2014

val reg3 = """(\d+)(-)(\d+)(-)(\d{4})""".r //22-08-2013

val reg4 = """(\w{3})\s(\d+)(,)\s(\d{4})""".r//Jan 11,2015

// Christopher|Jan 11, 2015|5
// Kapil|11 jan, 2015|5
// Thomas|6/17/2014|5
// J0hn|22-08-2013|5

val feedbackSplit = feedbackRDD.map(line=>line.split('|'))
val feedbackSplit = feedbackRDD.map(line => line.split('|'))

val validRecords = feedbackSplit.filter(x => (reg1.pattern.matcher(x(1).trim).matches|reg2.pattern.matcher(x(1).trim).matches|reg3.pattern.matcher(x(1).trim).matches|reg4.pattern.matcher(x(1).trim).matches))

val badRecords = feedbackSplit.filter(x => !(reg1.pattern.matcher(x(1).trim).matches|reg2.pattern.matcher(x(1).trim).matches|reg3.pattern.matcher(x(1).trim).matches|reg4.pattern.matcher(x(1).trim).matches))

bad.collect.foreach(println)

val valid = validRecords.map(rec=>(rec(0),rec(1),rec(2)))
val bad = badRecords.map(rec=>(rec(0),rec(1),rec(2)))


valid.collect.foreach(println)

valid.repartition(1).saveAsTextFile("spark9/valid.txt")
bad.repartition(1).saveAsTextFile("spark9/bad.txt")





val one = sc.textFile("spark16/file1.txt")

val two = sc.textFile("spark16/file2.txt")

one.collect.foreach(println)

two.collect.foreach(println)

val oneMap = one.map{_.split(",") match{case Array(a,b,c) => (a,(b,c))}}
val twoMap = two.map{_.split(",") match{case Array(a,b,c) => (a,(b,c))}}


val joined = oneMap.join(twoMap)


joined.collect.foreach(println)


val sum = joined.map{case(_,((_,num2),(_,_)))=>num2.toInt}.reduce(_+_)



val field = sc.textFile("spark15/tile1.txt")

field.collect.foreach(println)

val mapper = field.map(rec=>rec.split(','))

//.map(x=>x.map(x=>{if(x.isEmpty) 0 else x}))

mapper.collect.foreach(println)


type Scorecollector = (Int, Double)

val createScoreCombiner = (collector: Scorecollector, score: Double) => {
val (numberScores, totalScores) = collector
(numberScores+1, totalScores+score)
}

val scoreMerger = (collector1: Scorecollector, collector2: Scorecollector) =>{
val (numberScores1, totalScores1) = collector1
val (numberScores2, totalScores2) = collector2
(numberScores1+numberScores2, totalScores1+totalScores2)
}



val a = sc.parallelize(List(1,2,1,3),1)
val b = a.map((_,"b"))
val c = a.map((_,"c"))



spark-shell --master yarn \
--conf spark.ui.port=12654 \
--num-executors 1 \
--executor-memory 512M


a.collect.foreach(println)
1
2
1
3


b.collect.foreach(println)
(1,b)
(2,b)
(1,b)
(3,b)



c.collect.foreach(println)
(1,c)
(2,c)
(1,c)
(3,c)




c.cogroup(b).collect

Array[(Int, (Iterable[String], Iterable[String]))] =
Array(
(1,(CompactBuffer(b, b),CompactBuffer(c, c))),
(3,(CompactBuffer(b),CompactBuffer(c))),
(2,(CompactBuffer(b),CompactBuffer(c)))
)


val anRDD = sc.parallelize(List(1,2,3,4,5,6,7,8,2,4,2,1,1,1,1,1))


val anRDDMap = anRDD.groupBy(identity).mapValues(_.size)

anRDDMap.sortByKey().collect.foreach(println)

anRDD.countByValue


val a=sc.parallelize(1 to 10, 3)

val b = a.filter(_%2==0)
b.collect

val c=b.filter(_<8).map(rec=>rec/2)
c.collect


val a=sc.parallelize(List("dog","tiger","lion","cat","panther","eagle"))
val b=a.map(x=>(x.length,x))

b.groupByKey().map(rec=>(rec._1, (rec._2.map(x=>x)))).collect

b.foldByKey("")(_+_).collect

b.fold().collect



val pairedRDD1 = sc.parallelize(List(("cat",2),("cat",5),("book",4),("cat",12)))
val pairedRDD2 = sc.parallelize(List(("cat",2),("cup",5),("mouse",4),("cat",12)))


pairedRDD1.fullOuterJoin(pairedRDD2).collect


val a= sc.parallelize(1 to 20, 4)


a.collect

val b = a.glom.collect.foreach(println) foreach(_.take(5).foreach(println))


val a = sc.parallelize(1 to 9, 3)

val even = a.filter(_%2==0).collect
val odd = a.filter(_%2!=0).collect

a.groupBy(x=>if(x%2==0) "even" else "odd").collect



val a = sc.parallelize(List("dog","tiger","lion","cat","spider","eagle"),2)
val b = a.keyBy(_.length)

b.groupByKey().collect


val x = sc.parallelize(1 to 20)
val y = sc.parallelize(10 to 30)

val z = x.intersection(y)

z.collect


val a=sc.parallelize(List("dog","salmon","salmon","rat","elephant"),3)
val b=a.keyBy(_.length)

b.collect

val c=sc.parallelize(List("dog","cat","gnu","salmon","rabbit","turkey","wolf","bear","bee"),3)
val d=c.keyBy(_.length)

d.collect

b.join(d).collect

b.leftOuterJoin(d).collect


val a=sc.parallelize(List("dog","tiger","lion","cat","panther","eagle"),2)
val b=a.keyBy(_.length)

b.mapValues(rec=>"x"+rec+"X").collect


b.reduceByKey(_+_).collect


spark-shell --master yarn \
--conf spark.ui.port=12654 \
--num-executors 1 \
--executor-memory 512M


val a = sc.parallelize(List("dog","salmon","salmon","rat","elephant"),3)
val b = a.keyBy(_.length)
val c = sc.parallelize(List("dog","cat","gnu","salmon","rabbit", "turkey","wolf","bear","bee"),3)
val d = c.keyBy(_.length)


b.rightOuterJoin(d).collect



val a = sc.parallelize(List("dog","cat","owl","gnu","ant"),2)
val b = sc.parallelize(1 to a.count.toInt, 2)

val c = a.zip(b)


val a=sc.parallelize(List("dog","tiger","lion","cat","spider","eagle"),2)
val b=a.keyBy(_.length)

val c = sc.parallelize(List("ant", "falcon","squid"),2)
val d=c.keyBy(_.length)


b.subtractByKey(d).collect

val contentRDD = sc.textFile("Content.txt")

val contentRDDWordCount= contentRDD.flatMap(rec=>rec.split(" ")).map(word=>(word,1)).reduceByKey(_+_)

val result = contentRDDWordCount
// .collect.foreach(println)

result.saveAsTextF





val rddByKey=grpBykey.map{case(k,v)=>k->sc.makeRDD(v.toSeq)}

rddByKey.foreach{case(k,rdd)=>rdd.saveAsTextFile("emp"+k)}




val header =headerandrow.first
val data = =headerandrow.filter(_(0)!=header(0))
val maps=data.map(splits=>header.zip(data).toMap)

val result =maps.filter(maps("id") "myself")



spark-shell --master yarn \
--conf spark.ui.port=12654 \
--num-executors 1 \
--executor-memory 512M

sc.getConf.getAll.foreach(println)

val orders = sc.textFile("/public/retail_db/orders")
orders.take(10).foreach(println)

"/public/retail_db/orders"

"/public/retail_db/order_items"

val orderitems = sc.textFile("/public/retail_db/order_items")

orders.map(order=>(order.split(",")(3),"")).countByKey.foreach(println)
orders.map(order=>(order.split(",")(3),"")).take(10).foreach(println)


orders.map(order=>(order.split(",")(3))).countByKey.foreach(println)

val orderitems = sc.textFile("/public/retail_db/order_items")

orderitems.map(oi=>(oi.split(",")(4).toFloat)).take(10).foreach(println)

val orderitemsRevenue = orderitems.map(oi=>(oi.split(",")(4).toFloat))

orderitemsRevenue.reduce((total, revenue)=> total+revenue)

orderitemsRevenue.reduce((max, revenue)=> {
if (max<revenue) revenue else max
})

val orderitemsMap=orderitems.map(oi=>(oi.split(",")(1).toInt,oi.split(",")(4).toFloat))
val orderitemsGBK=orderitemsMap.groupByKey
orderitemsGBK.take(10).foreach(println)


orderitemsGBK.map(rec=>rec._2.toList.sum).take(10).foreach(println)

orderitemsGBK.map(rec=>(rec._1,rec._2.toList.sum)).take(10).foreach(println)

l.sortBy(o => -o)

val ordersSortedByRevenue = orderitemsGBK.
flatMap(rec=>{
rec._2.toList.sortBy(o => -o).map(k=>(rec._1,k))
})
ordersSortedByRevenue.take(10).foreach(println)

val orderitems = sc.textFile("/public/retail_db/order_items")
val orderitemsMap = orderitems.map(oi=>(oi.split(",")(1).toInt,oi.split(",")(4).toFloat))


orderitemsMaps.take(10).foreach(println)

val revenuePerOrderId = orderitemsMaps.reduceByKey((total, revenue)=>total+revenue)


revenuePerOrderId.take(10).foreach(println)


val minrevenuePerOrderId = orderitemsMaps.
reduceByKey((min, revenue)=>if (min>revenue) revenue else min)

minrevenuePerOrderId.take(10).foreach(println)

orderitemsMap.sortByKey().take(10).foreach(println)

revenuePerOrderId.sortByKey().take(10).foreach(println)

val revenueAndMaxPerProductID=orderitemsMap.
aggregateByKey((0.0,0.0))(
(inter, subtotal)=>(inter._1+subtotal, if(subtotal>inter._2) subtotal else inter._2),
(total, inter)=>(total._1+inter._1, if(total._2>inter._2) total._2 else inter._2)
)
revenueAndMaxPerProductID.take(10).foreach(println)

val revenueAndMaxPerProductID=orderitemsMap.
aggregateByKey((0.0f,0.0f))(
(inter, subtotal)=>(inter._1+subtotal, if(subtotal>inter._2) subtotal else inter._2),
(total, inter)=>(total._1+inter._1, if(total._2>inter._2) total._2 else inter._2)
)

orders.take(10).foreach(println)

val ordersMap=orders.map(rec=>(rec.split(",")(3),1))
ordersMap.take(10).foreach(println)

val orderResult=ordersMap.aggregateByKey(0)((acc,value)=>acc+1,(acc,value)=>acc+value)

orderResult.take(10).foreach(println)


spark-shell --master yarn \
--conf spark.ui.port=12654 \
--num-executors 1 \
--executor-memory 512M


val file1 = sc.textFile("spark16/file1.txt")
val file2 = sc.textFile("spark16/file2.txt")

file1.collect.foreach(println)

val content1 = file1.map(rec=>(rec.split(",")(0),rec.split(",")(1),rec.split(",")(2)))


val content1 = file1.map(_.split(",", -1) match{case Array(a,b,c)=>(a,(b,c))})
val content2 = file2.map(_.split(",", -1) match{case Array(a,b,c)=>(a,(b,c))})

content1.take(10).foreach(println)

val content1 = file1.map(_.split(",") match{case Array(a,b,c)=>(a,(b,c))})
content1.collect.foreach(println)

val content2 = file2.map(_.split(",") match{case Array(a,b,c)=>(a,(b,c))})
content2.collect.foreach(println)

val joined = content1.join(content2)
joined.collect.foreach(println)


val sum = joined.map{case (_,((_,num2),(_,_)))=>num2.toInt}.reduce(_+_)
sum.first







val sum = joined.map{case ((_,((_,num2),(_,_))))=>num2.toInt}.reduce(_+_)

sum


val field = sc.textFile("spark15/tile1.txt")

field.collect.foreach(println)

val mapper = field.map(x =>x.split(",",-1))

mapper.map(x=>x.map(x=>{if(x.isEmpty) 0 else x})).collect

mapper.take(10).foreach(println)


mapper.map(x=>x.map(x=>{if(x.isEmpty) 0 else x})).collect



mapper.map(x=>x.map(x=>{if(x.isEmpty) 0 else x})).collect



val au1 = sc.parallelize(List(("a",Array(1,2)),("b",Array(1,2))))
val au2 = sc.parallelize(List(("a",Array(3)),("b",Array(2))))

val joined = au1.join(au2)

joined.collect.fo


val union = au1.union(au2)


union.collect




val file = sc.textFile("spark10/sales.txt")

file.collect.foreach(println)


case class Employee(dep:String, des:String, cost:Double, state:String)

val employees= file.map(_.split(",")).map(x =>Employee(x(0),x(1),x(2).toDouble,x(3)))


val keyVals = employees.map(emp=>((emp.dep,emp.des, emp.state),(1, emp.cost)))

keyVals.collect.foreach(println)


val result = keyVals.reduceByKey{(a,b)=>(a._1+b._1,a._2+b._2)}

result.collect.foreach(println)


val file1 = sc.textFile("spark10/sales.txt")

file1.collect.foreach(println)


case class Employee(dep:String, des:String, cost:Double, state: String)


val employees = file1.map(_.split(",")).map(x=>Employee(x(0),x(1),x(2).toDouble,x(3)))

val keyVals = employees.map(emp=>((emp.dep,emp.des, emp.state),(1,emp.cost)))

val result = keyVals.reduceByKey{(a,b)=>(a._1+b._1,a._2+b._2)}

result.collect.foreach(println)




val grouped = sc.parallelize(Seq(((1,"two"),List((3,4),(5,6)))))

val flattened = grouped.flatMap{case (key, groupValues)=>groupValues.map{value=>(key._1,key._2, value._1, value._2)}}

flattened.collect



Seq(
(
(1,"two"),List((3,4),(5,6))
)
)





val file1 = sc.textFile("spark11/file1.txt")
val file2 = sc.textFile("spark11/file2.txt")
val file3 = sc.textFile("spark11/file3.txt")
val file4 = sc.textFile("spark11/file4.txt")


file4.collect.foreach(println)

val content1 = file1.flatMap(rec=>rec.split(" ")).
map(word=>(word,1)).
reduceByKey(_+_).
map(item=>item.swap).
sortByKey(false).map(item=>item.swap)


content2.collect.foreach(println)


val content2 = file2.flatMap(rec=>rec.split(" ")).
map(word=>(word,1)).
reduceByKey(_+_).
map(item=>item.swap).
sortByKey(false).map(item=>item.swap)


val content3 = file3.flatMap(rec=>rec.split(" ")).
map(word=>(word,1)).
reduceByKey(_+_).
map(item=>item.swap).
sortByKey(false).map(item=>item.swap)


val content4 = file4.flatMap(rec=>rec.split(" ")).
map(word=>(word,1)).
reduceByKey(_+_).
map(item=>item.swap).
sortByKey(false).map(item=>item.swap)


content3.collect.foreach(println)
content4.collect.foreach(println)


val name1 = file1.name
val name2 = file2.name
val name3 = file3.name
val name4 = file4.name


val array1 = content1.map(rec=>name1+"->"+rec._1+"-"+rec._2).collect
val array2 = content2.map(rec=>name2+"->"+rec._1+"-"+rec._2).collect
val array3 = content3.map(rec=>name3+"->"+rec._1+"-"+rec._2).collect
val array4 = content4.map(rec=>name4+"->"+rec._1+"-"+rec._2).collect

val file1word = sc.makeRDD(array1)
val file2word = sc.makeRDD(array2)
val file3word = sc.makeRDD(array3)
val file4word = sc.makeRDD(array4)


file3word.collect.foreach(println)


val union = file1word.union(file2word).union(file3word).union(file4word)

union.collect.foreach(println)

content1.map(rec=>rec._1).collect.foreach(println)


file1word.collect.foreach(println)


file1word.foreach(println)


file1word.repartition(1).saveAsTextFile("spark11/test.txt")


val file1 = sc.textFile("spark11/file1.txt")
val file2 = sc.textFile("spark11/file2.txt")
val file3 = sc.textFile("spark11/file3.txt")
val file4 = sc.textFile("spark11/file4.txt")

content1.collect.foreach(println)

val content1 = file1.flatMap(rec=>rec.split(" ")).map(word=>(word,1)).reduceByKey(_+_).map(rec=>rec.swap).
sortByKey(false).map(rec=>rec.swap)

val content2 = file2.flatMap(rec=>rec.split(" ")).map(word=>(word,1)).reduceByKey(_+_).map(rec=>rec.swap).
sortByKey(false).map(rec=>rec.swap)

val content3 = file3.flatMap(rec=>rec.split(" ")).map(word=>(word,1)).reduceByKey(_+_).map(rec=>rec.swap).
sortByKey(false).map(rec=>rec.swap)

val content4 = file4.flatMap(rec=>rec.split(" ")).map(word=>(word,1)).reduceByKey(_+_).map(rec=>rec.swap).
sortByKey(false).map(rec=>rec.swap)

val name1 = file1.name
val name2 = file2.name
val name3 = file3.name
val name4 = file4.name


val array1 = content1.map(rec=>name1+"->"+rec._1+"-"+rec._2).collect
val array2 = content2.map(rec=>name2+"->"+rec._1+"-"+rec._2).collect
val array3 = content3.map(rec=>name3+"->"+rec._1+"-"+rec._2).collect
val array4 = content4.map(rec=>name4+"->"+rec._1+"-"+rec._2).collect


val file1word=sc.makeRDD(array1)
val file2word=sc.makeRDD(array2)
val file3word=sc.makeRDD(array3)
val file4word=sc.makeRDD(array4)


val union = file1word.union(file2word).union(file3word).union(file4word)

union.collect.foreach(println)

union.repartition(1).saveAsTextFile("spark11/result.txt")




val salary = sc.textFile("spark12/salary.txt")
val technology = sc.textFile("spark12/technology.txt")


salaryMap.collect.foreach(println)


val salaryMap = salary.map(rec=>rec.split(",",-1)).map(x=>x.map(x=>{if(x.isEmpty) 0 else x})).collect


val mapper = field.map(x =>x.split(",",-1))

mapper.map(x=>x.map(x=>{if(x.isEmpty) 0 else x})).collect


case class Salary(first:String, last:String, salary:Double)
case class Technology(first:String, last:String, technology:String)


val salaryMap = salary.map(_.split(",")).map(rec=>Salary(rec(0),rec(1), rec(2)))


val salaryMap = salary.map(_.split(",")).map(rec=>((rec(0),rec(1)), rec(2)))
val technologyMap = technology.map(_.split(",")).map(rec=>((rec(0),rec(1)), rec(2)))

joinedMap2.collect.foreach(println)


val joined = salaryMap.join(technologyMap)

val joinedMap = joined.map(rec=>(rec._1._1,rec._1._2,rec._2._1,rec._2._2))

joinedMap.repartition(1).saveAsTextFile("spark12/result.txt")

val joinedMap2 = joined.map(rec=>(rec._1,rec._2._1,rec._2._2))

joinedMap2.repartition(1).saveAsTextFile("spark12/result2.txt")




val rdd = sc.parallelize(List( ("Deeapak" , "male",4000), ("Deeapak" , "male" ,2000), ("Deepaika" , "female" ,2000), ("Deepak" , "female" ,2000), ("Deepak" , "male" ,1000), ("Neeta" , "female" ,2000)))

val rddMap = rdd.map(rec=>((rec._1,rec._2),rec._3))

val result = rddMap.reduceByKey(_+_).collect.foreach(println)
rddMap.collect.foreach(println)


result.repartition(1).saveAsTextFile("spark12B/result.txt")





spark-shell --master yarn \
--conf spark.ui.port=12654 \
--num-executors 1 \
--executor-memory 512M


val content = sc.textFile("spark3/sparkdir1/file1.txt,spark3/sparkdir2/file2.txt,spark3/sparkdir3/file3.txt")


content.first
content.take(10).foreach(println)
content.collect.foreach(println)


val flatContent = content.flatMap(rec => rec.split(" "))

val trimmedContent = flatContent.map(rec => rec.trim)

val removeRDD = sc.parallelize(List("a","the","an","as","a","with","this","these","is","are","in","for","to","and","the","of"))

val filtered =trimmedContent.subtract(removeRDD)

val pairedRDD = filtered.map(word => (word,1))

val wordCount = pairedRDD.reduceByKey(_+_)

swapped.take(10).foreach(println)

val swapped = wordCount.map(rec => rec.swap)

val sortedOutput = swapped.sortByKey(false)

sortedOutput.saveAsTextFile("spark3/result")


import org.apache.hadoop.io.compress.Gzipcodec
sortedOutput.saveAsTextFile("spark3/compressedresult",classOf[Gzipcodec])


sortedOutput.saveAsTextFile("spark3/compressedresultGzip",classOf[org.apache.hadoop.io.compress.GzipCodec])


sortedOutput.saveAsTextFile("spark3/compressedresultSnappy",classOf[org.apache.hadoop.io.compress.SnappyCodec])


spark-shell --master yarn \
--conf spark.ui.port=12654 \
--num-executors 1 \
--executor-memory 512M


val name = sc.textFile("spark5/EmployeeName.csv")
val salary = sc.textFile("spark5/Employeesalary.csv")

val namepairRDD = name.map(rec=>(rec.split(",")(0),rec.split(",")(1)))
val salarypairRDD = salary.map(rec=>(rec.split(",")(0),rec.split(",")(1)))

val joined = namepairRDD.join(salarypairRDD)

val keyRemoved = joined.values

keyRemoved.collect.foreach(println)


val swapped = keyRemoved.map(rec=>rec.swap)


val grpByKey = swapped.groupByKey().collect()

val rddByKey = grpByKey.map{case(k,v)=>k->sc.makeRDD(v.toSeq)}

rddByKey.foreach{case(k,rdd)=>rdd.saveAsTextFile("spark5/Emplyee"+k.trim)}

val swapped = keyRemoved.map(rec=>rec.swap)





val headers = headerAndrow.first

val data = headerAndrow.filter(_(0)!=headers(0))

val maps = data.map(splits=>splits.zip(headers).toMap)

val result = maps.filter(map=>map("id")"myself")


result.saveAsTextFile("spark6/result.txt")



val csv = sc.textFile("spark6/user.csv")

val headerAndRows = csv.map(rec=>rec.split(",").map(_.trim))

data.take(10).foreach(println)


val headers = headerAndRows.first

val data = headerAndRows.filter(_(0)!=headers(0))


val maps = data.map(splits=>headers.zip(splits).toMap)


val result = maps.filter(map=>map("id")!="myself")

result.take(10).foreach(println)

maps["id"].first

result.saveAsTextFile("spark6/result.txt")


spark-shell --master yarn \
--conf spark.ui.port=12654 \
--num-executors 1 \
--executor-memory 512M

val name = sc.textFile("spark8/data.csv")

name.sortByKey().take(100).foreach(println)

sortByKey

val namepairRDD = name.map(rec=>(rec.split(",")(0),rec.split(",")(1)))

val swapped = namepairRDD.map(rec=>rec.swap)

swapped.take(10).foreach(println)

val combinedOutput = namepairRDD.combineByKey(List(_),(x:List[String], y:String)=>y:x,(x:List[String], y:List[String])=>x:y)


spark-shell --master yarn \
--conf spark.ui.port=12654 \
--num-executors 1 \
--executor-memory 512M








val feedbackRDD = sc.textFile("spark9/feedback.txt")

feedbackRDD.collect.foreach(println)


val reg1 = """(\d+)\s(\w{3}+)(,)\s(\d{4})""".r   //11 Jan, 2015
val reg2 = """(\d+)(\/)(\d+)(\/)(\d{4})""".r //6/17/2014|5 
val reg3 = """(\d+)(-)(\d+)(-)(\d{4})""".r //22-08-2013|5 
val reg4 = """(\w{3})\s(\d+)\s(\d{4})""".r//Jan 11, 2015|5 


val reg1 = """(\d+)\s(\w{3})(,)\s(\d{4})""".r //11 Jan,2015

val reg2 = """(\d+)(/)(\d+)(/)(\d{4})""".r //6/17/2014

val reg3 = """(\d+)(-)(\d+)(-)(\d{4})""".r //22-08-2013

val reg4 = """(\w{3})\s(\d+)(,)\s(\d{4})""".r//Jan 11,2015

// Christopher|Jan 11, 2015|5 
// Kapil|11 jan, 2015|5
// Thomas|6/17/2014|5 
// J0hn|22-08-2013|5 

val feedbackSplit = feedbackRDD.map(line=>line.split('|'))
val feedbackSplit = feedbackRDD.map(line => line.split('|'))

val validRecords = feedbackSplit.filter(x => (reg1.pattern.matcher(x(1).trim).matches|reg2.pattern.matcher(x(1).trim).matches|reg3.pattern.matcher(x(1).trim).matches|reg4.pattern.matcher(x(1).trim).matches))

val badRecords = feedbackSplit.filter(x => !(reg1.pattern.matcher(x(1).trim).matches|reg2.pattern.matcher(x(1).trim).matches|reg3.pattern.matcher(x(1).trim).matches|reg4.pattern.matcher(x(1).trim).matches))

bad.collect.foreach(println)

val valid = validRecords.map(rec=>(rec(0),rec(1),rec(2)))
val bad = badRecords.map(rec=>(rec(0),rec(1),rec(2)))


valid.collect.foreach(println)

valid.repartition(1).saveAsTextFile("spark9/valid.txt")
bad.repartition(1).saveAsTextFile("spark9/bad.txt")





val one = sc.textFile("spark16/file1.txt")

val two = sc.textFile("spark16/file2.txt")

one.collect.foreach(println)

two.collect.foreach(println)

val oneMap = one.map{_.split(",") match{case Array(a,b,c) => (a,(b,c))}}
val twoMap = two.map{_.split(",") match{case Array(a,b,c) => (a,(b,c))}}


val joined = oneMap.join(twoMap)


joined.collect.foreach(println)


val sum = joined.map{case(_,((_,num2),(_,_)))=>num2.toInt}.reduce(_+_)



val field = sc.textFile("spark15/tile1.txt")

field.collect.foreach(println)

val mapper = field.map(rec=>rec.split(','))

//.map(x=>x.map(x=>{if(x.isEmpty) 0 else x}))

mapper.collect.foreach(println)


type Scorecollector = (Int, Double)

val createScoreCombiner = (collector: Scorecollector, score: Double) => {
	val (numberScores, totalScores) = collector
	(numberScores+1, totalScores+score)
}

val scoreMerger = (collector1: Scorecollector, collector2: Scorecollector) =>{
	val (numberScores1, totalScores1) = collector1
	val (numberScores2, totalScores2) = collector2
	(numberScores1+numberScores2, totalScores1+totalScores2)
}



val a = sc.parallelize(List(1,2,1,3),1)
val b = a.map((_,"b"))
val c = a.map((_,"c"))



spark-shell --master yarn \
--conf spark.ui.port=12654 \
--num-executors 1 \
--executor-memory 512M


a.collect.foreach(println)
1
2
1
3


b.collect.foreach(println)
(1,b)
(2,b)
(1,b)
(3,b)



c.collect.foreach(println)
(1,c)
(2,c)
(1,c)
(3,c)




c.cogroup(b).collect

Array[(Int, (Iterable[String], Iterable[String]))] = 
	Array(
		(1,(CompactBuffer(b, b),CompactBuffer(c, c))), 
		(3,(CompactBuffer(b),CompactBuffer(c))), 
		(2,(CompactBuffer(b),CompactBuffer(c)))
	)


val anRDD = sc.parallelize(List(1,2,3,4,5,6,7,8,2,4,2,1,1,1,1,1))


val anRDDMap = anRDD.groupBy(identity).mapValues(_.size)

anRDDMap.sortByKey().collect.foreach(println)

anRDD.countByValue


val a=sc.parallelize(1 to 10, 3)

val b = a.filter(_%2==0)
b.collect

val c=b.filter(_<8).map(rec=>rec/2)
c.collect


val a=sc.parallelize(List("dog","tiger","lion","cat","panther","eagle"))
val b=a.map(x=>(x.length,x))

b.groupByKey().map(rec=>(rec._1, (rec._2.map(x=>x)))).collect

b.foldByKey("")(_+_).collect

b.fold().collect



val pairedRDD1 = sc.parallelize(List(("cat",2),("cat",5),("book",4),("cat",12)))
val pairedRDD2 = sc.parallelize(List(("cat",2),("cup",5),("mouse",4),("cat",12)))


pairedRDD1.fullOuterJoin(pairedRDD2).collect


val a= sc.parallelize(1 to 20, 4)


a.collect

val b = a.glom.collect.foreach(println)  foreach(_.take(5).foreach(println))


val a = sc.parallelize(1 to 9, 3)

val even = a.filter(_%2==0).collect
val odd = a.filter(_%2!=0).collect

a.groupBy(x=>if(x%2==0) "even" else "odd").collect



val a = sc.parallelize(List("dog","tiger","lion","cat","spider","eagle"),2)
val b = a.keyBy(_.length)

b.groupByKey().collect


val x = sc.parallelize(1 to 20)
val y = sc.parallelize(10 to 30)

val z = x.intersection(y)

z.collect


val a=sc.parallelize(List("dog","salmon","salmon","rat","elephant"),3)
val b=a.keyBy(_.length)

b.collect

val c=sc.parallelize(List("dog","cat","gnu","salmon","rabbit","turkey","wolf","bear","bee"),3)
val d=c.keyBy(_.length)

d.collect

b.join(d).collect

b.leftOuterJoin(d).collect


val a=sc.parallelize(List("dog","tiger","lion","cat","panther","eagle"),2)
val b=a.keyBy(_.length)

b.mapValues(rec=>"x"+rec+"X").collect


b.reduceByKey(_+_).collect


spark-shell --master yarn \
--conf spark.ui.port=12654 \
--num-executors 1 \
--executor-memory 512M


val a = sc.parallelize(List("dog","salmon","salmon","rat","elephant"),3)
val b = a.keyBy(_.length)
val c = sc.parallelize(List("dog","cat","gnu","salmon","rabbit", "turkey","wolf","bear","bee"),3)
val d = c.keyBy(_.length)


b.rightOuterJoin(d).collect



val a = sc.parallelize(List("dog","cat","owl","gnu","ant"),2)
val b = sc.parallelize(1 to a.count.toInt, 2)

val c = a.zip(b)


val a=sc.parallelize(List("dog","tiger","lion","cat","spider","eagle"),2)
val b=a.keyBy(_.length)

val c = sc.parallelize(List("ant", "falcon","squid"),2)
val d=c.keyBy(_.length)


b.subtractByKey(d).collect

val contentRDD = sc.textFile("Content.txt")

val contentRDDWordCount= contentRDD.flatMap(rec=>rec.split(" ")).map(word=>(word,1)).reduceByKey(_+_)

val result = contentRDDWordCount
// .collect.foreach(println)

result.saveAsTextFile("spark70/Content_wordCount.txt")


val contentRDD=sc.textFile("Content.txt")

val wordCount = contentRDD.flatMap(rec=>rec.split(" ")).map(word=>(word,1)).reduceByKey(_+_)

val result = wordCount

result.saveAsTextFile("spark70/Content_wordCount.txt")





val contentRDD=sc.textFile("Content.txt")

val contentLines = contentRDD.flatMap(rec=>rec.split("\n"))

val noEmptyLines = contentLines.filter(_.length>0)

noEmptyLines.collect.foreach(println)

val noEmptyLinesMap = noEmptyLines.map(rec=>(rec,""))

noEmptyLines.saveAsTextFile("spark71/noEmptyLines.txt")

noEmptyLinesMap.saveAsSequenceFile("spark71/noEmptyLinesSequence.seq")


create table t0117x_employee2 (first_name varchar(20), last_name varchar(20));

insert into t0117x_employee2 sele


sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_export \
--username retail_user \
--password itversity \
--target-dir /user/matymar7/spark72/employee \
--table t0117x_employee \
--split-by employee_id




val sqlContext = new org.apache.spark.SQLContext


sqlContext.sql("use retail_export")

sqlContext.sql("select * from INFORMATION_SCHEMA.tables").show() where table_name like \"%t0117x%\"")

sqlContext.sql("use retail_export describe t0117x_employee").show()


val empRDD = sc.textFile("spark72/employee")


empRDD.map(_.split(",")).map(rec=>(rec(0),rec(1))).toDF("employee_id","name").show()


val empRDD=sqlContext.jsonFile("spark73/employee_origin.json")

val empDF=empRDD.toDF("First_Name","Last_Name")

empDF.registerTempTable("emp")

val result = sqlContext.sql("select First_Name, Last_Name from emp")

result.toJSON.saveAsTextFile("spark73/result.json")


val empRDD = sqlContext.jsonFile("spark73/employee_origin.json")

val empDF = empRDD.toDF("First_Name", "Last_Name")

empDF.registerTempTable("emp")

val result = sqlContext.sql("select First_Name, Last_Name from emp")

result.show()

result.collect.foreach(println)


result.toJSON.saveAsTextFile("spark73/result.json")


sqoop import-all-tables \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--warehouse-dir /user/matymar7/spark74/retail_db 
--m 1 


sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--table orders \
--target-dir /user/matymar7/spark74/orders \
--num-mappers 1


sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--table orders \
--target-dir /user/matymar7/spark74/orders \
--m 1



sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--table order_items \
--target-dir /user/matymar7/spark74/order_items \
--num-mappers 1


val ordersRDD = sc.textFile("spark74/orders")
val ordersDF = ordersRDD.map(_.split(",")).
map(rec=>(rec(0),rec(1),rec(2),rec(3))).
toDF("order_id", "order_date", "order_customer_id", "order_status")

ordersDF.show()

ordersDF.registerTempTable("orders")


+-------------------+-------------+------+-----+---------+----------------+
| Field             | Type        | Null | Key | Default | Extra          |
+-------------------+-------------+------+-----+---------+----------------+
| order_id          | int(11)     | NO   | PRI | NULL    | auto_increment |
| order_date        | datetime    | NO   |     | NULL    |                |
| order_customer_id | int(11)     | NO   |     | NULL    |                |
| order_status      | varchar(45) | NO   |     | NULL    |                |
+-------------------+-------------+------+-----+---------+----------------+



val orderitemsRDD = sc.textFile("spark74/order_items")
val orderitemsDF = orderitemsRDD.map(_.split(",")).
map(rec=>(rec(0),rec(1),rec(2),rec(3),rec(4),rec(5))).
toDF("order_item_id", "order_item_order_id", "order_item_product_id", "order_item_quantity","order_item_subtotal","order_item_product_price")


orderitemsDF.registerTempTable("orderitems")

orderitemsDF.show()

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




val result = sqlContext.sql("select o.order_id, o.order_date, oi.order_item_subtotal "+
"from orders o "+
"join orderitems oi on o.order_id=oi.order_item_order_id "+
"order by o.order_date desc")

result.show()

val resultNumOrders = sqlContext.sql("select o.order_date, count(o.order_id) as number_of_orders "+
"from orders o "+
"group by o.order_date "+
"order by o.order_date desc")

resultNumOrders.show()


result.toJSON.saveAsTextFile("spark74/result.json")
resultNumOrders.toJSON.saveAsTextFile("spark74/resultNumOrders.json")


sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--table order_items \
--target-dir /user/matymar7/spark75sql/order_items \
--num-mappers 1


val orderitemsRDD = sc.textFile("spark75sql/order_items")

val orderitemsDF = orderitemsRDD.map(_.split(",")).
map(rec=>(rec(0),rec(1),rec(2),rec(3),rec(4),rec(5))).
toDF("order_item_id","order_item_order_id","order_item_product_id","order_item_quantity","order_item_subtotal","order_item_product_price")

orderitemsDF.show()

orderitemsDF.registerTempTable("order_items")

val maxrevenue = sqlContext.sql("select max(order_item_subtotal) as maxrevenue from order_items")
val minrevenue = sqlContext.sql("select min(order_item_subtotal) as minrevenue from order_items")
val avgrevenue = sqlContext.sql("select avg(order_item_subtotal) as avgrevenue from order_items")

val results = sqlContext.sql("select "+
"max(order_item_subtotal) as max_revenue, "+
"min(order_item_subtotal) as min_revenue, "+
"avg(order_item_subtotal) as avg_revenue, "+
"sum(order_item_subtotal) as sum_revenue, "+
"count(order_item_id) as count_revenue, "+
"sum(order_item_subtotal)/count(order_item_id) as avg2_revenue "+
"from order_items")


maxrevenue.show()
minrevenue.show()
avgrevenue.show()

results.show()



sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--table orders \
--target-dir /user/matymar7/spark76/orders \
--num-mappers 1


val ordersRDD = sc.textFile("spark76/orders")

+-------------------+-------------+------+-----+---------+----------------+
| Field             | Type        | Null | Key | Default | Extra          |
+-------------------+-------------+------+-----+---------+----------------+
| order_id          | int(11)     | NO   | PRI | NULL    | auto_increment |
| order_date        | datetime    | NO   |     | NULL    |                |
| order_customer_id | int(11)     | NO   |     | NULL    |                |
| order_status      | varchar(45) | NO   |     | NULL    |                |
+-------------------+-------------+------+-----+---------+----------------+


val ordersMap = ordersRDD.map(_.split(",")).map(rec=>(rec(3), 1))




countByKey
+-------------------+-------------+------+-----+---------+----------------+

ordersMap.countByKey()


res7: scala.collection.Map[String,Long] = Map(PAYMENT_REVIEW -> 729, CLOSED -> 7556, SUSPECTED_FRAUD -> 1558, PROCESSING -> 8275, COMPLETE -> 22899, PENDING -> 7610, PENDING_PAYMENT -> 15030, ON_HOLD -> 3798, CANCELED -> 1428)



groupByKey
+-------------------+-------------+------+-----+---------+----------------+


ordersMap.groupByKey().map{case (k,v)=>(k, v.size)}

res34: Array[(String, Int)] = Array((PENDING_PAYMENT,15030), (CLOSED,7556), (CANCELED,1428), (PAYMENT_REVIEW,729), (PENDING,7610), (PROCESSING,8275), (ON_HOLD,3798), (SUSPECTED_FRAUD,1558), (COMPLETE,22899))



reduceByKey
+-------------------+-------------+------+-----+---------+----------------+


ordersMap.reduceByKey(_+_).collect

res35: Array[(String, Int)] = Array((PENDING_PAYMENT,15030), (CLOSED,7556), (CANCELED,1428), (PAYMENT_REVIEW,729), (PENDING,7610), (ON_HOLD,3798), (PROCESSING,8275), (SUSPECTED_FRAUD,1558), (COMPLETE,22899))



aggregateByKey
+-------------------+-------------+------+-----+---------+----------------+

ordersMap.aggregateByKey(0)((acc,value)=>acc+1,(acc,value)=>acc+value)

res40: Array[(String, Int)] = Array((PENDING_PAYMENT,15030), (CLOSED,7556), (CANCELED,1428), (PAYMENT_REVIEW,729), (PENDING,7610), (ON_HOLD,3798), (PROCESSING,8275), (SUSPECTED_FRAUD,1558), (COMPLETE,22899))



combineByKey
+-------------------+-------------+------+-----+---------+----------------+

ordersMap.combineByKey(
(mark) => (mark, 1),
(acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
(acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
).map(rec=>(rec._1,rec._2._1)).collect


res55: Array[(String, (Int, Int))] = Array((PENDING_PAYMENT,(15030,15030)), (CLOSED,(7556,7556)), (CANCELED,(1428,1428)), (PAYMENT_REVIEW,(729,729)), (PENDING,(7610,7610)), (ON_HOLD,(3798,3798)), (PROCESSING,(8275,8275)), (SUSPECTED_FRAUD,(1558,1558)), (COMPLETE,(22899,22899)))

// http://apachesparkbook.blogspot.com/2015/12/combiner-in-pair-rdds-combinebykey.html





sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--table orders \
--target-dir /user/matymar7/spark78/orders \
--m 1



sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--table order_items \
--target-dir /user/matymar7/spark78/order_items \
--num-mappers 1


val ordersRDD=sc.textFile("spark78/orders")
val orderitemsRDD=sc.textFile("spark78/order_items")


+-------------------+-------------+------+-----+---------+----------------+
| Field             | Type        | Null | Key | Default | Extra          |
+-------------------+-------------+------+-----+---------+----------------+
| order_id          | int(11)     | NO   | PRI | NULL    | auto_increment |
| order_date        | datetime    | NO   |     | NULL    |                |
| order_customer_id | int(11)     | NO   |     | NULL    |                |
| order_status      | varchar(45) | NO   |     | NULL    |                |
+-------------------+-------------+------+-----+---------+----------------+

val ordersDF=ordersRDD.map(_.split(",")).
map(rec=>(rec(0),rec(1),rec(2),rec(3))).
toDF("order_id","order_date","order_customer_id","order_status")


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


val orderitemsDF=orderitemsRDD.map(_.split(",")).
map(rec=>(rec(0),rec(1),rec(2),rec(3),rec(4),rec(5))).
toDF("order_item_id","order_item_order_id","order_item_product_id","order_item_quantity","order_item_subtotal","order_item_product_price")

ordersDF.registerTempTable("orders")
orderitemsDF.registerTempTable("orderitems")


val result1 = sqlContext.sql("select o.order_customer_id, o.order_date, sum(oi.order_item_subtotal) as revenue "+
"from orders o "+
"join orderitems oi on o.order_id=oi.order_item_order_id "+
"group by o.order_customer_id, o.order_date "+
"order by o.order_customer_id")



result1.toJSON.saveAsTextFile("spark78/result_customer_orderdate.json")


val result2 = sqlContext.sql("select o.order_customer_id, max(oi.order_item_subtotal) as max_revenue "+
"from orders o "+
"join orderitems oi on o.order_id=oi.order_item_order_id "+
"group by o.order_customer_id "+
"order by o.order_customer_id")

result2.toJSON.saveAsTextFile("spark78/result_customer.json")









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


sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--table products \
--target-dir /user/matymar7/spark79/products \
--num-mappers 1


val productsRDD=sc.textFile("spark79/products")
val productsDF=productsRDD.map(_.split(",")).
map(rec=>(rec(0),rec(1),rec(2),rec(3),rec(4),rec(5))).
toDF("product_id","product_category_id","product_name","product_description","product_price","product_image")

productsDF.show()

productsDF.registerTempTable("products")

val result = sqlContext.sql("select * from products where product_price is not null")

result.top(5).show()


val filtered = productsRDD.map(_.split(",")).filter(rec=>rec(4).length>0)

val sortByPrice = filtered.map(rec=>(rec(4),rec(2))).sortByKey(false)

sortByPrice.collect


sortByPrice.top(5)

sortByPrice.takeOrdered(10)(Ordering[Int].reverse)

sortByPrice.takeOrdered(5)( Ordering[Int])








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





sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--table products \
--target-dir /user/matymar7/spark80/products \
--num-mappers 1






val productsRDD = sc.textFile("spark80/products")
val productsMap = productsRDD.filter(rec=>rec.split(",")(4)!="").map(_.split(","))





val productsCat = productsMap.map(rec=>(rec(1),(rec(2),rec(4)))).groupByKey()

productsCat.mapValues(rec=>rec.toList.sorted).collect

productsCat.mapValues(rec=>rec.map(rec2=>rec2.swap).toList.sorted).collect


productsCat.mapValues(rec=>rec.map(rec2=>rec2.swap).toList.sorted).take(10).foreach(println)


productsCat.
map(rec=>{
rec._2.map(rec2=>(rec2._2,rec2))sortByKey()
}).collect.foreach(println)


val test = List(5,3,2,4,1)

productsMap.collect.foreach(_.foreach(println))


val productsDF = productsMap.
map(rec=>(rec(0),rec(1),rec(2),rec(3),rec(4),rec(5))).
toDF("product_id","product_category_id","product_name","product_description","product_price","product_image")



productsDF.registerTempTable("products")


val productarray = sqlContext.sql("select product_category_id, product_name, product_price from products")

val new = sc.makeRDD(productarray)


productarray.toString
productarray.map(_.split(","))
.collect



val productRDD = sc.textFile("spark81/product.csv")

productRDD.collect.foreach(println)


val productMap = productRDD.map(_.split(","))
val productDF = productMap.
map(rec=>(rec(0).trim.toInt,rec(1),rec(2),rec(3).trim.toInt,rec(4).trim.toFloat)).
toDF("productID","productCode","name","quantity","price")


productDF.registerTempTable("product")

val product = sqlContext.sql("select * from product")
// productID, productCode, name, quantity, price

// save data to Hive

sqlContext.sql("CREATE DATABASE matymar7")
sqlContext.sql("CREATE TABLE matymar7.product81 "+
"(productID int, productCode string, name string, quantity int, price float) "+
"STORED AS orc")


product.insertInto("matymar7.product81")


sqlContext.sql("CREATE TABLE matymar7.product81_parquet "+
"(productID int, productCode string, name string, quantity int, price float) "+
"STORED AS parquet")


product.insertInto("matymar7.product81_parquet")


// describe formatted product81

hdfs dfs -ls hdfs://nn01.itversity.com:8020/apps/hive/warehouse/matymar7.db/product81



val product = sqlContext.sql("select * from product")


val product2000 = product.where("quantity<=2000").show()


product.where("productCode in ('PEN')").select("productCode","name", "price").show()

val productPEN = product.where("productCode in ('PEN')").select("name", "price").show()

// val productPENCIL = product.where("productCode like 'pencil%')").show()

val productPENCIL = sqlContext.sql("select * from product where lower(name) like 'pencil%'").show()

val productP = sqlContext.sql("select * from product where lower(name) like 'p% %r%'").show()




val result=sqlContext.sql("select * from product where quantity>=5000 and lower(name) like 'p%'").show()
val result=sqlContext.sql({
"select * from product where quantity>=5000 and price<1.24 and lower(name) like 'pen%'"}).show()

val result=sqlContext.sql({
"select * from product where quantity<=5000 and lower(name) not like 'pen%'"}).show()

val result=sqlContext.sql({
"select * from product where lower(name) in ('pen red', 'pen black')"}).show()

val result=sqlContext.sql({
"select * from product where (price between 1.0 and 2.0) and (quantity between 1000 and 2000)"}).show()


val result=sqlContext.sql({
"select * from product where lower(name) in ('pen red', 'pen black')"}).show()


val result=sqlContext.sql({
"select * from product where productCode is null"}).show()

val result=sqlContext.sql({
"select * from product where lower(name) like 'pen%' order by price desc"}).show()


val result=sqlContext.sql({
"select * from product where lower(name) like 'pen%' order by price desc, quantity asc"}).show()


val result=sqlContext.sql({
"select * from product order by price desc limit 2"}).show()


+---------+-----------+---------+--------+-------+
|productID|productCode|     name|quantity|  price|
+---------+-----------+---------+--------+-------+
|     1004|        PEC|Pencil HB|       0|9999.99|
|     1002|        PEN| pen Blue|    8000|    1.0|
+---------+-----------+---------+--------+-------+


val result=sqlContext.sql("select concat(productCode,'-',name),price from product").show()

val result=sqlContext.sql("select concat(productCode,'-',name),price from product").show()


val result=sqlContext.sql("select distinct price from product").show()


val result=sqlContext.sql("select distinct price, name from product").show()


val result=sqlContext.sql("select concat(productCode,'-',name),price from product").show()

val result=sqlContext.sql({
"select * from product order by productCode, productID"}).show()


val result=sqlContext.sql({
"select count(productID) from product"}).show()


val result=sqlContext.sql({
"select productCode,count(productID) from product group by productCode"}).show()


val result=sqlContext.sql("select max(quantity) as max,"+
"min(quantity) as min,"+
"avg(quantity) as avg,"+
"std(quantity) as std,"+
"sum(quantity) as total "+
"from product").show()


val result=sqlContext.sql("select max(price) as max,"+
"min(price) as min,"+
"productCode "+
"from product "+
"group by productCode").show()


val result=sqlContext.sql("select productCode,"+
"max(quantity) as max,"+
"min(quantity) as min,"+
"cast(avg(quantity) as decimal(7,2)) as avg,"+
"cast(std(quantity) as decimal(7,2)) as std,"+
"sum(quantity) as total "+
"from product "+
"group by productCode").show()


val result=sqlContext.sql("select productCode,"+
"count(*) as count,"+
"cast(avg(price) as decimal(7,2)) as avg "+
"from product "+
"group by productCode "+
"having count(productID)>=3").show()


val result=sqlContext.sql("select max(price) as max,"+
"min(price) as min,"+
"avg(price) as avg,"+
"sum(price) as total,"+
"productCode "+
"from product "+
"group by productCode with rollup").show()





hdfs dfs -ls spark87
Found 2 items
-rw-r--r--   2 matymar7 hdfs         72 2019-03-03 12:05 spark87/Supplier.csv
-rw-r--r--   2 matymar7 hdfs        323 2019-03-03 12:03 spark87/product87.csv




val supplierRDD = sc.textFile("spark87/Supplier.csv")
val product87RDD = sc.textFile("spark87/product87.csv")
val productsupplierRDD = sc.textFile("spark87/products_suppliers.csv")

val productMap = product87RDD.map(_.split(","))
val productDF = productMap.
map(rec=>(rec(0).trim.toInt,rec(1),rec(2),rec(3).trim.toInt,rec(4).trim.toFloat,rec(5).trim.toInt)).
toDF("productID","productCode","name","quantity","price","supplierid")

val supplierMap = supplierRDD.filter(rec=>rec.size>0).map(_.split(","))
val supplierDF = supplierMap.
map(rec=>(rec(0).trim.toInt,rec(1),rec(2))).
toDF("Supplierid","name","phone")


val productsupplierMap = productsupplierRDD.filter(rec=>rec.size>0).map(_.split(","))
val productsupplierDF = productsupplierMap.
map(rec=>(rec(0).trim.toInt,rec(1).trim.toInt)).
toDF("productID","supplierID")


productDF.registerTempTable("product")
supplierDF.registerTempTable("supplier")
productsupplierDF.registerTempTable("productsupplier")


val result = sqlContext.sql("select * from product").show()
val result = sqlContext.sql("select * from supplier").show()
val result = sqlContext.sql("select * from productsupplier").show()


val result = sqlContext.sql("select p.name, "+
"p.price, "+
"s.name as supplier "+
"from product p "+
"join productsupplier ps on p.productID=ps.productID "+
"join supplier s on s.supplierID=ps.Supplierid "+
"where p.price<0.6").show()



productsupplierDF.collect.foreach(println)


// problem 88

val result = sqlContext.sql("select p.name, "+
"p.price, "+
"s.name as supplier "+
"from productsupplier ps "+
"join product p on p.productID=ps.productID "+
"join supplier s on s.supplierID=ps.Supplierid ").show()


val result = sqlContext.sql("select "+
"s.name as supplier "+
"from productsupplier ps "+
"join product p on p.productID=ps.productID "+
"join supplier s on s.supplierID=ps.Supplierid "+
"where lower(p.name) like 'pencil 3b'").show()


val result = sqlContext.sql("select p.name "+
"from productsupplier ps "+
"join product p on p.productID=ps.productID "+
"join supplier s on s.supplierID=ps.Supplierid "+
"where lower(s.name) like 'abc traders'").show()



val patientsRDD = sc.textFile("spark89/patients.csv")
val patientsDF = patientsRDD.map(_.split(",")).
map(rec=>(rec(0),rec(1),rec(2),rec(3))).
toDF("patientID","name","dateOfBirth","lastVisitDate")

patientsDF.show()

patientsDF.registerTempTable("patients")

sqlContext.sql("select * from patients").show()
sqlContext.sql("select * from patients where lastVisitDate between '2012-09-21' and NOW()").show()
sqlContext.sql("select * from patients where year(date(dateOfBirth))=2011").show()

sqlContext.sql("select *,cast(datediff(NOW(),date(dateOfBirth))/365 as decimal(7,2)) as age from patients").show()

sqlContext.sql("select *,datediff(NOW(),date(lastVisitDate)) as lastvisitdays from patients where datediff(NOW(),date(lastVisitDate))>2360").show()

sqlContext.sql("select * from (select *,cast(datediff(NOW(),date(dateOfBirth))/365 as decimal(7,2)) as age from patients) as main where age<=18").show()

sqlContext.sql("select * from (select *,cast(datediff(NOW(),date(dateOfBirth))/365 as decimal(7,2)) as age from patients) as main where age>18").show()
patientsRDD.collect.foreach(println)


// -rw-r--r--   2 matymar7 hdfs         24 2019-03-03 14:04 spark90/Fee.txt
// -rw-r--r--   2 matymar7 hdfs         25 2019-03-03 14:04 spark90/course1.txt

val course=sc.textFile("spark90/course1.txt")
val fee=sc.textFile("spark90/Fee.txt")


val courseMap =course.filter(_.size>0).map(_.split(",")).map(rec=>(rec(0).trim.toInt,rec(1))).toDF("id","course")
val feeMap =fee.filter(_.size>0).map(_.split(",")).map(rec=>(rec(0).trim.toInt,rec(1).trim.toInt)).toDF("id","fee")

courseMap.show()
feeMap.show()

courseMap.registerTempTable("course")
feeMap.registerTempTable("fee")

val result1 = sqlContext.sql("select c.id, c.course, f.fee from course c left join fee f on c.id=f.id").show()
val result1 = sqlContext.sql("select c.id, c.course, f.fee from course c right join fee f on c.id=f.id").show()
val result1 = sqlContext.sql("select c.id, c.course, f.fee from course c left join fee f on c.id=f.id where f.fee is not null").show()




// spark91
// -rw-r--r--   2 matymar7 hdfs        275 2019-03-03 14:22 spark91/employee.json
// -rw-r--r--   2 matymar7 hdfs        267 2019-03-03 14:21 spark91/employee_origin.json


val employee = sqlContext.read.json("spark91/employee_origin.json")

val result = employee.toDF("FirstName","LastName")

result.toJSON.saveAsTextFile("spark91/result.json")



