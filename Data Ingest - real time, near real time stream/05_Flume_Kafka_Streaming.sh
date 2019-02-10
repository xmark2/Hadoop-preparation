#//****Flume and Kafka streaming***//

#login to the server
ssh matymar7@itversity.com

cd flume_demo/
ls -ltr
#copy
cp -rf wslogstohdfs wslogstokafka
cd wslogstokafka
ls -ltr

vi wshdfs.conf

##rename the agent

########################################
# Name the components on this agent 
wk.sources = ws 
wk.sinks = kafka 
wk.channels = mem 

# Describe/configure the source 
wk.sources.ws.type = exec 
wk.sources.ws.command = tail -F /opt/gen_logs/logs/access.log 

# Describe the sink 

# Use a channel which buffers events in memory 
wk.channels.mem.type = memory 
wk.channels.mem.capacity = 1000 
wk.channels.mem.transactioncapacity = 100 

# Bind the source and sink to the channel 
wk.sources.ws.channels = mem 
wk.sinks.kafka.channel = mem 


:%s/wh/wk 
############################################


mv wshdfs.conf wskafka.conf
rm wshdfs.conf.working
vi wskafka.conf


########################################
# Name the components on this agent 
wk.sources = ws 
wk.sinks = kafka 
wk.channels = mem 

# Describe/configure the source 
wk.sources.ws.type = exec 
wk.sources.ws.command = tail -F /opt/gen_logs/logs/access.log 

# Describe the sink 
wk.sinks.kafka.type = org.apache.flume.sink.kafka.KafkaSink
wk.sinks.kafka.brokerList = nn01.itversity.com:6667, nn02.itversity.com:6667, rm01.itversity.com:6667
wk.sinks.kafka.topic = fkdemodg

# Use a channel which buffers events in memory 
wk.channels.mem.type = memory 
wk.channels.mem.capacity = 1000 
wk.channels.mem.transactioncapacity = 100 

# Bind the source and sink to the channel 
wk.sources.ws.channels = mem 
wk.sinks.kafka.channel = mem 


:%s/wh/wk 
############################################






#//****Validate***//

flume-ng agent --name wk --conf-file /home/matymar7/flume_demo/wslogstokafka/wskafka.conf

##new terminal
history|grep consume

kafka-console-consumer.sh --zookeeper nn01.itversity.com:6667, nn02.itversity.com:6667, rm01.itversity.com:6667
 --topic fkdemodg --from-beginning


tail -F /opt/gen_logs/logs/access.log | nc -lk gw01.itversity.com 9999

ssh su - root

cd /hdp01/kafka-logs/fkdemodg-0/
ls -ltr
view 000000000000000000.log 

##open terminal again
ssh
kafka-console-consumer.sh --zookeeper nn01.itversity.com:6667, nn02.itversity.com:6667, rm01.itversity.com:6667
 --topic fkdemodg --from-beginning