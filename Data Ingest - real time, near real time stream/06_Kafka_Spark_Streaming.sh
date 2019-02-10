#//****Flume and Spark streaming***//

#//based on doc we will update build.sbt
#//local pc
cd /Research/code/scalademo/retail
vi build.sbt

################
name :="retail"
version := "1.0"
scalaversion := "2.10.6"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.6.3"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.6.3"
libraryDependencies += "org.apache.spark" % "spark-streaming-flume_2.10" % "1.6.3"
libraryDependencies += "org.apache.spark" % "spark-streaming-flume-sink_2.10" % "1.6.3"
libraryDependencies += "org.scala-lang" % "scala-library" % "2.10.6"
libraryDependencies += "org.apache.commons" % "commons-langs3" % "3.3.2"
libraryDependencies += "org.apache.commons" % "spark-streaming-kafka_2.10" % "1.6.3"


#################

sbt.package

#copy
cp StreamingDepartmentCount.scala KafkaStreamingDepartmentCount.scala

vi KafkaStreamingDepartmentCount.scala

##based on doc update our code

###############################


import org.apache.spark.SparkConf
import org.apache.spark.streaming.{StreamingContext.Seconds}
import org.apache.spark.streaming.kafka._
import kafka.serializer.StringDecoder

object KafkaStreamingDepartmentCount {
	def main(args: Array[String]) {
		val conf = new SparkConf().
			setAppName("Streaming word count").
			setMaster(arg(0))
		val ssc = new StreamingContext(conf, second(30))

		val kafkaParams = map[String, String]("metadata.broker.list" -> "nn01.itversity.com:6667, nn02.itversity.com:6667, rm01.itversity.com:6667")
		val topicSet = Set("fkdemodg")

		val stream = KafkaUtils.
		 createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet)

		val messages = stream.map(s=>s._2)
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

#######

cd ../../..
sbt package

##copy our jar file and connecting to the lab

scp target/scala-2.10/retail_2.10-1.0.jar matymar7@gw01.itversity.com 

ssh matymar7@itversity.com 

cd flume_demo
ls -ltr 
cd wslogstokafka
vi wskafka.conf
flume-ng agent -n wk -f /home/matymar7/flume_demo/wslogstokafka/wskafka.conf


##now let's deploy to the cluster

spark-submit \
 --class KafkaStreamingDepartmentCount \
 --master yarn \
 --conf spark.ui.port=12689 \
 --jars "/usr/hdp/2.5.0.0-1245/kafka/libs/kafka_2.10-0.8.2.1.jar, "+ 
 "/usr/hdp/2.5.0.0-1245/kafka/libs/spark-streaming-flume_2.10-1.6.2.jar, "+
 "/usr/hdp/2.5.0.0-1245/kafka/libs/metrics-core-2.2.0.jar" \
 retail_2_10-1.0.jar yarn-client


 ##open another terminal / session to validate

 ssh ...itversity.com 

 hdfs dfs -ls /user/matymar7/deptwisetraffic
 hdfs dfs -cat /user/matymar7/deptwisetraffic/cnt-..../part*



