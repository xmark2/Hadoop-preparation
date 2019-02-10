//****Spark streaming***//


//diff contexts in Spark
//SparkContext vs StreamingContext

spark-shell --master yarn --conf spark.ui.port=12456

sc 
//org.apache.spark.SparkContext ...

sqlContext
//org.apache.spark.sql.SQLContext ...


import org.apache.spark.streaming.
//check

import org.apache.spark.SparkConf

val conf = new SparkConf().setAppName("streaming").setMaster("yarn-client")
val ssc = new StreamingContext(conf, second(10))

//it will fail, because spark is also running streaming context

//first stop the spark context 
sc.stop

//then configure the object
val conf = new SparkConf().setAppName("streaming").setMaster("yarn-client")

//create the streaming context
val ssc = new StreamingContext(conf, second(10))






//web-service demo --netcat

//one terminal
nc -lk 9999

//second terminal
nc localhost 9999




//**Spark streaming --- Develop Word Count Program ***//

/*
streaming word count
	dev the project
	compile into jar
	start netcat service
	run application using jar
*/


//check sbt and make sure scala and spark using the same version
cat build.sbt


sbt console


import org.apache.spark.SparkConf
import org.apache.spark.streaming._

val conf = new SparkConf().setAppName("Streaming word count").setMaster("local")
val ssc = new StreamingContext(conf, second(10))

val lines = ssc.socketTextStream("localhost", 9999)
val words = lines.flatMap(line => line.split(" "))
val tuples = words.map(word => (word, 1))
val wordCounts = tuples.reduceByKey((t,v) => t + v)

wordCounts.print()

ssc.start()
ssc.awaitTermination()




//type and copy this code
//###############################


import org.apache.spark.SparkConf
import org.apache.spark.streaming._

object StreamingWordCount {
	def main(args: Array[String]) {
		val executionMode = args(0)
		val conf = new SparkConf().setAppName("Streaming word count").setMaster(executionMode)
		val ssc = new StreamingContext(conf, second(10))

		val lines = ssc.socketTextStream(args(1), args(2).toInt)
		val words = lines.flatMap(line => line.split(" "))
		val tuples = words.map(word => (word, 1))
		val wordCounts = tuples.reduceByKey((t,v) => t + v)

		wordCounts.print()

		ssc.start()

	}
}

//#######

//now let's go the following path and paste the code and ":x" save it
vi src/main/scala/StreamingWordCount.scala


//now let's create the jar file with sbt
sbt package


//**Spark streaming --- Ship and Run Word Count Program on Cluster ***//

cd Research/code/scalademo/retail/
vi src/main/scala/StreamingWordCount.scala

//add awaitTermination to our code

//###############################


import org.apache.spark.SparkConf
import org.apache.spark.streaming._

object StreamingWordCount {
	def main(args: Array[String]) {
		val executionMode = args(0)
		val conf = new SparkConf().setAppName("Streaming word count").setMaster(executionMode)
		val ssc = new StreamingContext(conf, second(10))

		val lines = ssc.socketTextStream(args(1), args(2).toInt)
		val words = lines.flatMap(line => line.split(" "))
		val tuples = words.map(word => (word, 1))
		val wordCounts = tuples.reduceByKey((t,v) => t + v)

		wordCounts.print()

		ssc.start()
		ssc.awaitTermination()

	}
}

//#######

//now rebuild
sbt package

//check the jar file created
scp target/scala-2.10/retail_2.10-1.0.jar matymar7@gw01.itversity.com 



//open a new terminal for web service
nc -lk gw01.itversity.com 9999

//go back to the 1st terminal and run the following
telnet gw01.itversity.com 9999

//quit from telnet
quit


spark-submit \
 --class StreamingWordCount \
 --master yarn \
 --conf spark.ui.port=12657 \
 retail_2_10-1.0.jar yarn-client gw01.itversity.com 9999


//go to the second terminal 
//and type some words as well that we will track in the other terminal with counting
nc -lk gw01.itversity.com 9999
Hello world
let us test word count
program to get number of words
which are streaming every 10 seconds

//*the first terminal will shows*/
/*
(get,1)
(program,1)
(number,1)
...
*/


//**Spark streaming --- Problem & Solution ***//


/*
Problem
	-read data from retail_db logs
	-compute department traffic every 30 seconds
	-save the output to HDFS
Solution
	-use Spark Streaming
	-publish messages from retail_db logs to netcat
	-create Dstream
	-process and save the output
*/



//here is our log file
ls -ltr /opt/get_logs/logs/

//we can validate our log
tail_logs.sh 


//Solution
//go to our local pc
vi src/main/scala/StreamingDepartmentCount.scala


//###############################


import org.apache.spark.SparkConf
import org.apache.spark.streaming.{StreamingContext.Seconds}

object StreamingDepartmentCount {
	def main(args: Array[String]) {
		val conf = new SparkConf().
			setAppName("Streaming word count").
			setMaster(arg(0))
		val ssc = new StreamingContext(conf, second(30))

		val messages = ssc.socketTextStream(args(1), args(2).toInt)
		val departmentMessages = messages.
		 filter(msg => {
		 	val endPoint = msg.split(" ")(6)
		 	endPoint.split("/")(1) == "department"
		 })
		val departments = messages.
		 filter(msg => {
		 	val endPoint = msg.split(" ")(6)
		 	endPoint.split("/")(1)
		 })
		val departmentTraffic = departments.
		 reduceByKey((total,value) => total + value)

		departmentTraffic.saveAsTextFiles("/user/matymar7/deptwisetraffic/cnt")

		ssc.start()
		ssc.awaitTermination()
	}
}

//#######


sbt package

//copy the jar file to Cluster server:
//1st login to the server
ssh matymar7@gw01.itversity.com 

tail_logs.sh 

//let's create a web-service to our log
tail_logs.sh|nc -lk gw01.itversity.com 8123

//open another session to check the web-service
telnet gw01.itversity.com 8123

//quit from telnet
quit

//go to the 1st terminal where we can run our jar on cluster with web-service
spark-submit \
 --class StreamingDepartmentCount \
 --master yarn \
 --conf spark.ui.port=12678 \
 retail_2.10-1.0.jar \
 yarn-client gw01.itversity.com 8123


//check our web-service location on the cluster
hdfs dfs -ls /user/matymar7


hdfs dfs -ls /user/matymar7/deptwisetraffic

//you can find bunch of files here
//check one of them
