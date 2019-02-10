//****Flume and Spark streaming***//

/*
read data from /opt/gen_logs/logs/access.log using flume
write unprocessed data as well as streaming depart count data to HDFS

development
-update build.sbt
-create new program
-compile and build jar

run and validate
-ship it to the cluster
-run flume agent
-run spark submit with jars
-validate whether files are being generated or not

*/

cd flume_demo/wslogstohdfs
cd flume_demo/
mkdir strdeptcount
cd strdeptcount

cd ..
cd ..
vi wslogstohdfs.conf


########################################
# Name the components on this agent 
wh.sources = ws 
wh.sinks = hdfs 
wh.channels = mem 

# Describe/configure the source 
wh.sources.ws.type = exec 
wh.sources.ws.command = tail -F /opt/gen_logs/logs/access.log 

# Describe the sink 
wh.sinks.hd.type = hdfs
wh.sinks.hd.hdfs.path = hdfs://nn01.itversity.com:8020/user/matymar7/flume_demo

wh.sinks.hd.hdfs.filePrefix = FlumeDemo 
wh.sinks.hd.hdfs.fileSuffix = .txt
wh.sinks.hd.hdfs.rollInternal = 120
wh.sinks.hd.hdfs.rollSize = 1048576
wh.sinks.hd.hdfs.rollCount = 100
wh.sinks.hd.hdfs.fileType = DataStream

# Use a channel which buffers events in memory 
wh.channels.mem.type = memory 
wh.channels.mem.capacity = 1000 
wh.channels.mem.transactioncapacity = 100 

# Bind the source and sink to the channel 
wh.sources.ws.channels = mem 
wh.sinks.hd.channel =mem


:x
############################################

//later we will add one more channel and one more sink to this file

cp wslogstohdfs/wshdfs.conf strdeptcount/sdc.conf 
cd strdeptcount/
vi sdc.conf 


//now renaming the wh agent to sdc

########################################
# Name the components on this agent 
sdc.sources = ws 
sdc.sinks = hdfs 
sdc.channels = mem 

# Describe/configure the source 
sdc.sources.ws.type = exec 
sdc.sources.ws.command = tail -F /opt/gen_logs/logs/access.log 

# Describe the sink 
sdc.sinks.hd.type = hdfs
sdc.sinks.hd.hdfs.path = hdfs://nn01.itversity.com:8020/user/matymar7/flume_demo

sdc.sinks.hd.hdfs.filePrefix = FlumeDemo 
sdc.sinks.hd.hdfs.fileSuffix = .txt
sdc.sinks.hd.hdfs.rollInternal = 120
sdc.sinks.hd.hdfs.rollSize = 1048576
sdc.sinks.hd.hdfs.rollCount = 100
sdc.sinks.hd.hdfs.fileType = DataStream

# Use a channel which buffers events in memory 
sdc.channels.mem.type = memory 
sdc.channels.mem.capacity = 1000 
sdc.channels.mem.transactioncapacity = 100 

# Bind the source and sink to the channel 
sdc.sources.ws.channels = mem 
sdc.sinks.hd.channel = mem


:%s/wh/sdc
############################################



//now the sdc.sinks to hd spark and channel to hdmem sparkmem
//and memory has to be hdmem then copy the 3 mem lines and create sparkmem channels

########################################
# Name the components on this agent 
sdc.sources = ws 
sdc.sinks = hd spark 
sdc.channels = hdmem sparkmem 

# Describe/configure the source 
sdc.sources.ws.type = exec 
sdc.sources.ws.command = tail -F /opt/gen_logs/logs/access.log 

# Describe the sink 
sdc.sinks.hd.type = hdfs
sdc.sinks.hd.hdfs.path = hdfs://nn01.itversity.com:8020/user/matymar7/flume_demo

sdc.sinks.hd.hdfs.filePrefix = FlumeDemo 
sdc.sinks.hd.hdfs.fileSuffix = .txt
sdc.sinks.hd.hdfs.rollInternal = 120
sdc.sinks.hd.hdfs.rollSize = 1048576
sdc.sinks.hd.hdfs.rollCount = 100
sdc.sinks.hd.hdfs.fileType = DataStream

# Use a channel which buffers events in memory 
sdc.channels.hdmem.type = memory 
sdc.channels.hdmem.capacity = 1000 
sdc.channels.hdmem.transactioncapacity = 100 

sdc.channels.sparkmem.type = memory 
sdc.channels.sparkmem.capacity = 1000 
sdc.channels.sparkmem.transactioncapacity = 100 

# Bind the source and sink to the channel 
sdc.sources.ws.channels = hdmem sparkmem 
sdc.sinks.hd.channel = hdmem
sdc.sinks.spark.channel = sparkmem

:%s/wh/sdc
############################################


cd /usr/hdp/2.5.0.0-1245/flume/lib
ls -ltr 

//we need to add this file to sdc config file

ls -ltr |grep scala

//make sure all the 3 jar files available in flume

//check spark streaming+ flume integration guide documentation
/* copy this from documentation

agent.sinks = spark
agent.sinks.spark.type = org.apache.spark.streaming.flume.sink.sparksink
agent.sinks.spark.hostname = <hostname>
agent.sinks.spark.port = <port>
agent.sinks.spark.channel = memoryChannel

*/

cd flume_demo/
cd flume_demo/strdeptcount
vi sdc.conf 

//add to the sink the doc part of the code
//sink is already named as hd spark so we keep the following 3 lines in
//rename the agent to sdc

########################################
# Name the components on this agent 
sdc.sources = ws 
sdc.sinks = hd spark 
sdc.channels = hdmem sparkmem 

# Describe/configure the source 
sdc.sources.ws.type = exec 
sdc.sources.ws.command = tail -F /opt/gen_logs/logs/access.log 

# Describe the sink 
sdc.sinks.hd.type = hdfs
sdc.sinks.hd.hdfs.path = hdfs://nn01.itversity.com:8020/user/matymar7/flume_demo

sdc.sinks.hd.hdfs.filePrefix = FlumeDemo 
sdc.sinks.hd.hdfs.fileSuffix = .txt
sdc.sinks.hd.hdfs.rollInternal = 120
sdc.sinks.hd.hdfs.rollSize = 1048576
sdc.sinks.hd.hdfs.rollCount = 100
sdc.sinks.hd.hdfs.fileType = DataStream

sdc.sinks.spark.type = org.apache.spark.streaming.flume.sink.SparkSink
sdc.sinks.spark.hostname = gw01.itversity.com
sdc.sinks.spark.port = 8123

# Use a channel which buffers events in memory 
sdc.channels.hdmem.type = memory 
sdc.channels.hdmem.capacity = 1000 
sdc.channels.hdmem.transactioncapacity = 100 

sdc.channels.sparkmem.type = memory 
sdc.channels.sparkmem.capacity = 1000 
sdc.channels.sparkmem.transactioncapacity = 100 

# Bind the source and sink to the channel 
sdc.sources.ws.channels = hdmem sparkmem 
sdc.sinks.hd.channel = hdmem
sdc.sinks.spark.channel = sparkmem

:%s/wh/sdc
############################################


flume-ng agent -n sdc -f sdc.conf 





//based on doc we will update build.sbt
//local pc
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


#################

sbt package


cd /Research/code/scalademo/retail/src/main/scala
//copy this file and rename
cp StreamingDepartmentCount.scala FlumeStreamingDepartmentCount.scala

vi FlumeStreamingDepartmentCount.scala

//###############################

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{StreamingContext.Seconds}
import org.apache.spark.streaming.flume._

object FlumeStreamingDepartmentCount {
	def main(args: Array[String]) {
		val conf = new SparkConf().
			setAppName("Streaming word count").
			setMaster(arg(0))
		val ssc = new StreamingContext(conf, second(30))

		val stream = FlumeUtils.createPollingStream(ssc, args(1), args(2).toInt)
		val messages = stream.
		 map(s=> new String(s.event.getBody.array()))
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

cd ../../..
sbt package

//let's put our code to the cluster and run it there
scp target/scala-2.10/retail_2.10-1.0.jar matymar7@gw01.itversity.com

ls -ltr
rm matymar7@gw01.itversity.com
scp target/scala-2.10/retail_2.10-1.0.jar matymar7@gw01.itversity.com

//let' login to the server
ssh ...itversity.com

spark-submit \
 --class FlumeStreamingDepartmentCount \
 --master yarn \
 --conf spark.ui.port=12986 \
 --jars "/usr/hdp/2.5.0.0-1245/spark/lib/spark-streaming-flume-sink_2.10-1.6.2.jar, "+ 
 "/usr/hdp/2.5.0.0-1245/spark/lib/spark-streaming-flume_2.10-1.6.2.jar, "+
 "/usr/hdp/2.5.0.0-1245/spark/lib/commons-langs3-3.5.jar, "+
 "/usr/hdp/2.5.0.0-1245/spark/lib/flume-ng-sdk-1.5.2.2.5.0.0-1245.jar," \
 retail_2_10-1.0.jar yarn-client gw01.itversity.com 8123


//copy the code, go to home directory

flume-ng agent -n sdc -f sdc.conf 

//we need one more terminal to check which terminal is created

hdfs fs -ls /user/matymar7/deptwisetraffic

hdfs fs -ls /user/matymar7/deptwisetraffic/cnt-...
hdfs fs -cat /user/matymar7/deptwisetraffic/cnt-.../part*


