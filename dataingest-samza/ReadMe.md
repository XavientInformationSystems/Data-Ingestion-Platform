# Data Ingestion Platform(DiP) - Real time data analysis - Samza Streaming

“The previous blog "https://www.linkedin.com/pulse/real-time-data-ingestion-easy-simple-co-dev-neeraj-sabharwal" showed how we can leverage the power of Apache Storm and Kafka to do real time data analytics and visualization.

This blog focusses on using Samza Streaming for performing real time data analysis with the help of Apache Kafka.

Apache Samza is a distributed stream processing framework. It uses Apache Kafka for messaging, and Apache Hadoop YARN to provide fault tolerance, processor isolation, security, and resource management.

### Samza Streaming Features

  - Simple API
  - Managed state
  - Fault tolerance
  - Durability 
  - Scalability
  - Pluggable
  - Processor isolation


#### Prerequisites
The API has been tested on below mentioned HDP 2.4 components:
- Apache Hadoop 2.7.1.2.4
- Apache Kafka 0.9.0.2.4	
- Apache Samza 0.11.0
- Apache Hbase 1.1.2.2.4
- Apache Hive 1.2.1.2.4
- Apache Zeppelin 0.6.0.2.4
- Apache Tomcat Server 8.0
- Apache Phoenix 4.4.0.2.4
- Apache Maven 
- Java 1.8 or later

#### High Level Process Workflow with Apache Samza

![alt text](https://github.com/XavientInformationSystems/Data-Ingestion-Platform/blob/master/dataingest-samza/src/main/resources/images/Samza Architecture.png "Logo Title Text 1") 

- Input to the application can be fed from a user interface that allows you either enter data manually or upload the data  JSON for bulk processing
- Data ingested is published by the Kafka broker which streams the data to Kafka consumer process
- The content of the message is extracted from the kafka source  and is sent to different sinks for its persistence 
- Hive external table provides data storage through HDFS and Phoenix provides an SQL interface for HBase tables
- Reporting and visualization  of data is done through Zeppelin

### STEPS
  - Create a new group and a user in the local systen and map that user to HDFS using below mentioned commands:
```
# Run as hadoop superuser(hdfs)
hadoop dfs -mkdir /user/samza/samzaoutput

# Give permission to "samzaoutput" folder as it required later by user "hive"
hadoop fs -chmod -R 777 /user/samza/samzaoutput


``` 

- Go to $KAFKA_INSTALL_DIR/bin and create a Kafka topic named "samzatopic" using the below mentioned command
```
./kafka-topics.sh --create --topic samzatopic --zookeeper <zookeeper-server>:<port> --replication-factor 1 --partition 5
```

- Download the source code from <<<TODO>>> and compile the code using below commands:

```
# Decompress the zip file.
unzip samza-dip.zip

# Compile the code
cd samza-dip
mvn clean package

#Create Tar
mkdir -p deploy/samza
tar -xvf ./target/dip.samza-0.11.0-dist.tar.gz -C deploy/samza

#Place the tar file on HDFS and give permissions
hadoop fs -put target/dip.samza-0.11.0-dist.tar.gz /samza/code/dip.samza-0.11.0-dist.tar.gz

hadoop  fs -chmod 777 /samza/code/dip.samza-0.11.0-dist.tar.gz

```

```

# Deploy Application
deploy/samza/bin/run-job.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file:///dip-samza/src/main/resources/job.properties
```

- Once the application is deployed it can be open through yarn resource manager by clicking on Application Master Tracking UI of the application submitted

- The job submitted will look like this:

![alt text](https://github.com/XavientInformationSystems/Data-Ingestion-Platform/blob/master/dataingest-samza/src/main/resources/images/SamzaApplication.PNG "Logo Title Text 1") 

- Download the DataIngestUI source code from "www.URL" and start it using apache tomcat as shown below:

```
Commands for starting UI application
```

- Open the UI for the application by visiting the URL "http://<tomcat-server>:<port>/DataIngestGUI/UI.jsp" , it will look like this:

![alt text](https://github.com/XavientInformationSystems/Data-Ingestion-Platform/blob/master/dataingest-flink/src/main/resources/images/DataIngestUI.PNG "Logo Title Text 1") 

- Now you can send the data through a kafka producer using the command
  /kafka-console-producer.sh --broker-list <<kafkahost>>:<<brokerport>> --topic samzatopic
- . Below mentioned are some sample data rows:
    - JSON
       {"text": "text" , "source": "source" , "reTweeted": "reTweeted" , "username": "username" , "createdAt": "createdAt" , "retweetCount": "retweetCount" , "userLocation": "userLocation" , "inReplyToUserId": "inReplyToUserId" , "inReplyToStatusId": "inReplyToStatusId" , "userScreenName": "userScreenName" , "userDescription": "userDescription" , "userFriendsCount": "userFriendsCount" , "userStatusesCount": "userStatusesCount" , "userFollowersCount": "userFollowersCount"}
   

Data can be viewed in Zeppelin by
- Create an external table in Hive using the below command so as the view the data in zeppelin
- Create a view in phoenix by accessing "sqlline.py" to access the data written in Hbase using below command-


```

- You can draw various charts/graphs in Zeppelin as shown below:
 
![alt text](https://github.com/XavientInformationSystems/Data-Ingestion-Platform/blob/master/dataingest-samza/src/main/resources/images/ZeppelinView.PNG "Logo Title Text 1") .