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

kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test

## setup a unique name for topic

kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic kafkademodg

## we also need to configure our zookeeper

kafka-topics.sh --create --zookeeper nn01.itversity.com:2181, nn02.itversity.com:2181, rm01.itversity.com:2181 --replication-factor 1 --partitions 1 --topic kafkademodg


## to validate topic is created run the cmd from documentation

##to list all topics
kafka-topics.sh --list --zookeeper nn01.itversity.com:2181, nn02.itversity.com:2181, rm01.itversity.com:2181

##to check the new topic
kafka-topics.sh --list --zookeeper nn01.itversity.com:2181, nn02.itversity.com:2181, rm01.itversity.com:2181 --topic kafkademodg



################

kafka-topics.sh --create \
 --zookeeper nn01.itversity.com:2181, nn02.itversity.com:2181, rm01.itversity.com:2181 
 --replication-factor 1 
 --partitions 1 
 --topic kafkademodg


kafka-topics.sh --list \
 --zookeeper nn01.itversity.com:2181, nn02.itversity.com:2181, rm01.itversity.com:2181 
 --topic kafkademodg

kafka-console-producer.sh \
 --broker-list localhost:9092 \
 --topic kafkademodg

###Ambari, check the broker-list and check the port number

kafka-console-producer.sh \
 --broker-list nn01.itversity.com:6667, nn02.itversity.com:6667, rm01.itversity.com:6667 \
 --topic kafkademodg


##new version of consumer
kafka-console-consumer.sh \
 --bootstrap-server nn01.itversity.com:6667, nn02.itversity.com:6667, rm01.itversity.com:6667 \
 --topic kafkademodg \
 --from-beginning

##0.9 version of consumer

kafka-console-consumer.sh \
 --zookeeper nn01.itversity.com:6667, nn02.itversity.com:6667, rm01.itversity.com:6667 \
 --topic kafkademodg \
 --from-beginning



##open a new terminal for consumer

##another terminal for producer
##now our streaming works well





####summary note

kafka-topics.sh --create \
 --zookeeper nn01.itversity.com:2181, nn02.itversity.com:2181, rm01.itversity.com:2181 
 --replication-factor 1 
 --partitions 1 
 --topic kafkademodg

kafka-topics.sh --list \
 --zookeeper nn01.itversity.com:2181, nn02.itversity.com:2181, rm01.itversity.com:2181 
 --topic kafkademodg

kafka-console-producer.sh \
 --broker-list nn01.itversity.com:6667, nn02.itversity.com:6667, rm01.itversity.com:6667 \
 --topic kafkademodg

##0.9 version of consumer
kafka-console-consumer.sh \
 --zookeeper nn01.itversity.com:6667, nn02.itversity.com:6667, rm01.itversity.com:6667 \
 --topic kafkademodg \
 --from-beginning
