#***Create Data Frame and Register as Temp table*

cd /opt/gen_logs/

#logs stored here//
ls -ltr


tail -F 
tail -F access.log 

tail_logs.sh 

tail -F /opt/gen_logs/logs/access.log 


#Flume - web server logs to HDFS - source exec

mkdir 

mkdir wslogstohdfs 
cp example.conf wslogstohdfs/ 
cd wslogstohdfs/ 
mv example.conf wshdfs.conf 
vi wshdfs.conf

#Change a1 agent name to wh
########################################
# Name the components on this agent 
a1.sources = rl 
a1.sinks = k1 
a1.channels = c1 

# Describe/configure the source 
a1.sources.rl.type = netcat 
a1.sources.rl.bind = localhost 
a1.sources.rl.port = 44444 

# Describe the sink 
a1.sinks.k1.type = logger

# Use a channel which buffers events in memory 
a1.channels.c1.type = memory 
a1.channels.c1.capacity = 1000 
a1.channels.c1.transactioncapacity = 100 

# Bind the source and sink to the channel 
a1.sources.rl.channels = c1 
a1.sinks.k1.channel = c1 


:%/a1/wh
########################################

:%/r1/ws
:%/c1/mem
###Result
########################################
# Name the components on this agent 
wh.sources = ws 
wh.sinks = k1 
wh.channels = mem 

# Describe/configure the source 
wh.sources.ws.type = netcat 
wh.sources.ws.bind = locwhhost 
wh.sources.ws.port = 44444 

# Describe the sink 
wh.sinks.k1.type = logger

# Use a channel which buffers events in memory 
wh.channels.mem.type = memory 
wh.channels.mem.capacity = 1000 
wh.channels.mem.transactioncapacity = 100 

# Bind the source and sink to the channel 
wh.sources.ws.channels = mem 
wh.sinks.k1.channel = mem 





#Change the source type to exec

###Result
########################################
# Name the components on this agent 
wh.sources = ws 
wh.sinks = k1 
wh.channels = mem 

# Describe/configure the source 
wh.sources.ws.type = exec 
wh.sources.ws.command = tail -F /opt/gen_logs/logs/access.log 

# Describe the sink 
wh.sinks.k1.type = logger

# Use a channel which buffers events in memory 
wh.channels.mem.type = memory 
wh.channels.mem.capacity = 1000 
wh.channels.mem.transactioncapacity = 100 

# Bind the source and sink to the channel 
wh.sources.ws.channels = mem 
wh.sinks.k1.channel = mem 

:x
########################################


flume-ng agent -n wh -f /home/matymar7/flume_demo/wslogstohdfs/wshdfs.conf

######################################################################################
#######  Flume - web server logs to HDFS - sink HDFS

cd /home/matymar7/flume_demo/wslogstohdfs
vi  wshdfs.conf

:%/a1/wh

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

# Use a channel which buffers events in memory 
wh.channels.mem.type = memory 
wh.channels.mem.capacity = 1000 
wh.channels.mem.transactioncapacity = 100 

# Bind the source and sink to the channel 
wh.sources.ws.channels = mem 
wh.sinks.hdfs.channel = mem 


:x
############################################


##Let's check where the path coming from

hdfs dfs -rm -r /user/matymar7/flume_demo
cd /etc//hadoop/conf 
vi core-site.xml

#<value>hdfs://nn01.itversity.com:8020</value>



#Now let's back to our file
cd ~/flume_demo/wslogstohdfs/
vi wshdfs.conf
set -o vi

flume-ng agent -n wh -f /home/matymar7/flume_demo/wslogstohdfs/wshdfs.conf

# Open a new terminal

hdfs fs -ls /user/matymar7/flume_demo

# Open one of the file
hdfs fs -cat /user/matymar7/flume_demo/FlumeData.15113...




########################################
# Name the components on this agent 
wh.sources = ws 
wh.sinks = hd 
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
wh.sinks.hd.channel = mem 


:x
############################################


set -o vi

flume-ng agent -n wh -f /home/matymar7/flume_demo/wslogstohdfs/wshdfs.conf


# go to the 2nd terminal

hdfs fs -ls /user/matymar7/flume_demo

# Open one of the file
hdfs fs -cat /user/matymar7/flume_demo/FlumeData.15113... .txt
hdfs fs -cat /user/matymar7/flume_demo/FlumeData.15113... .txt|wc -l


#Now validate once again
hdfs fs -ls /user/matymar7/flume_demo
cd flume_demo/wslogstohdfs/
vi wshdfs.conf

##Change to interval, default takes every 30 seconds
#1st terminal
flume-ng agent -n wh -f /home/matymar7/flume_demo/wslogstohdfs/wshdfs.conf
#2nd terminal
hdfs fs -ls /user/matymar7/flume_demo


hdfs fs -cat /user/matymar7/flume_demo/FlumeData.15113... .txt | wc -l
