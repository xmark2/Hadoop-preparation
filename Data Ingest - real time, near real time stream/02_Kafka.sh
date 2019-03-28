#***Kafka getting started*


check the current version 
cd /usr/hdp/current/kafka-broker/
cd libs

cd /usr/hdp/current/kafka-broker/bin
ls -ltr

##check path of working directory
pwd 

## to validate our bash profile we need to run the following
. ~/.bash_profile

## type kafka to list all the kafka bash codes
kafka

##now copy the code from documentation

kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test

## setup a unique name for topic

kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic matymar7

## we also need to configure our zookeeper

kafka-topics --create --zookeeper nn01.itversity.com:2181,nn02.itversity.com:2181,rm01.itversity.com:2181 --replication-factor 1 --partitions 1 --topic matymar7


## to validate topic is created run the cmd from documentation

##to list all topics
kafka-topics --list --zookeeper nn01.itversity.com:2181,nn02.itversity.com:2181,rm01.itversity.com:2181

##to check the new topic
kafka-topics --list --zookeeper nn01.itversity.com:2181,nn02.itversity.com:2181,rm01.itversity.com:2181 --topic matymar7



################

kafka-topics --create \
 --zookeeper nn01.itversity.com:2181,nn02.itversity.com:2181,rm01.itversity.com:2181 \
 --replication-factor 1 \
 --partitions 1 \
 --topic matymar7


kafka-topics --list \
 --zookeeper nn01.itversity.com:2181,nn02.itversity.com:2181,rm01.itversity.com:2181 \
 --topic matymar7

kafka-console-producer \
 --broker-list localhost:9092 \
 --topic matymar7

###Ambari,check the broker-list and check the port number

kafka-console-producer \
 --broker-list nn01.itversity.com:6667,nn02.itversity.com:6667,rm01.itversity.com:6667 \
 --topic matymar7


##new version of consumer
kafka-console-consumer \
 --bootstrap-server nn01.itversity.com:6667,nn02.itversity.com:6667,rm01.itversity.com:6667 \
 --topic matymar7 \
 --from-beginning

##0.9 version of consumer

kafka-console-consumer \
 --zookeeper nn01.itversity.com:6667,nn02.itversity.com:6667,rm01.itversity.com:6667 \
 --topic matymar7 \
 --from-beginning



##open a new terminal for consumer

##another terminal for producer
##now our streaming works well





####summary note 0.9 version

kafka-topics --create \
 --zookeeper nn01.itversity.com:2181,nn02.itversity.com:2181,rm01.itversity.com:2181 \
 --replication-factor 1 \
 --partitions 1 \
 --topic matymar7

kafka-topics --list \
 --zookeeper nn01.itversity.com:2181,nn02.itversity.com:2181,rm01.itversity.com:2181 \
 --topic matymar7

kafka-console-producer \
 --broker-list nn01.itversity.com:6667,nn02.itversity.com:6667,rm01.itversity.com:6667 \
 --topic matymar7

##0.9 version of consumer
kafka-console-consumer \
 --zookeeper nn01.itversity.com:6667,nn02.itversity.com:6667,rm01.itversity.com:6667 \
 --topic matymar7 \
 --from-beginning






####summary note 2.0 version

kafka-topics --create \
 --zookeeper nn01.itversity.com:2181,nn02.itversity.com:2181,rm01.itversity.com:2181 \
 --replication-factor 1 \
 --partitions 1 \
 --topic matymar7

kafka-topics --list \
 --zookeeper nn01.itversity.com:2181,nn02.itversity.com:2181,rm01.itversity.com:2181 \
 --topic matymar7

kafka-console-producer \
 --broker-list nn01.itversity.com:6667,nn02.itversity.com:6667,rm01.itversity.com:6667 \
 --topic matymar7

##0.9 version of consumer
kafka-console-consumer \
 --bootstrap-server nn01.itversity.com:6667,nn02.itversity.com:6667,rm01.itversity.com:6667 \
 --topic matymar7 \
 --from-beginning