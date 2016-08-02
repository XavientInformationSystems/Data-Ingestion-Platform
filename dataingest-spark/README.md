# Data Ingestion Platform(DiP) - Real time data analysis - Spark Streaming

The previous blog "https://www.linkedin.com/pulse/real-time-data-ingestion-easy-simple-co-dev-neeraj-sabharwal" showed how we can leverage the power of Apache Storm and Kafka to do real time data analytics and visualization.

This blog focusses on using Spark Streaming for performing real time data analysis with the help of Apache Kafka.

Spark Streaming is the next level of the core Spark API that enables high-throughput, flexible, fault-tolerant stream processing of live data streams.

### Spark Streaming Features

  - Micro Batch Model
  - Java, Scala, client bindings
  - Declarative API
  - Very High Throughput 
  - Memory Partitioning
  - Extensive community support

#### Prerequisites
The API has been tested on below mentioned HDP 2.4 components:
- Apache Hadoop 2.7.1.2.4
- Apache Kafka 0.9.0.2.4	
- Apache Spark 1.6.1
- Apache Hbase 1.1.2.2.4
- Apache Hive 1.2.1.2.4
- Apache Zeppelin 0.6.0.2.4
- Apache Tomcat Server 8.0
- Apache Phoenix 4.4.0.2.4
- Apache Maven 
- Java 1.7 or later

#### High Level Process Workflow with Spark-Streaming

![alt text](https://github.com/XavientInformationSystems/Data-Ingestion-Platform/blob/master/dataingest-spark/src/main/resources/images/SparkArchitecture.PNG "Logo Title Text 1") 

- Input to the application can be fed from a user interface that allows you either enter data manually or upload the data in XML, JSON or CSV file format for bulk processing
- Data ingested is published by the Kafka broker which streams the data to Kafka consumer process
- Once the message type is identified, the content of the message is extracted and is sent to different executors
- Hive external table provides data storage through HDFS and Phoenix provides an SQL interface for HBase tables
- Reporting and visualization  of data is done through Zeppelin

### STEPS
  - Create a new group and a user in the local systen and map that user to HDFS using below mentioned commands:
```
# Run as root user.
# Create a group
groupadd -g GID hadoopusers

# Add user to that group
useradd -g hadoopusers -u UID spark

# Set password for that user
passwd spark

# Run as hadoop superuser(hdfs)
hdfs dfs -mkdir /user/spark
hdfs dfs -chown -R spark:hadoopusers /user/spark
hdfs dfsadmin -refreshUserToGroupsMappings
hadoop dfs -mkdir /user/spark/sparkstreaming

# Give permission to "sparkstreaming" folder as it required later by user "hive"
hadoop fs -chmod -R 777 /user/spark/sparkstreaming
``` 

- Go to $KAFKA_INSTALL_DIR/bin and create a Kafka topic named "visits" using the below mentioned command
```
./kafka-topics.sh --create --topic spark_topic --zookeeper <zookeeper-server>:<port> --replication-factor 1 --partition 5
```

- Download the source code from https://github.com/XavientInformationSystems/Data-Ingestion-Platform/tree/master/dataingest-spark and compile the code using below commands:

```
# Decompress the zip file.
unzip spark-kafka.zip

# Compile the code
cd spark-kafka
mvn clean package
```

- Once the code has being successfully compiled, go to the target directory and locate a jar by the name "uber-dip.spark-1.0.0.jar"
- Copy the jar file in hdfs and then submit it to spark for initiating your streaming application using the below command:

```
spark-submit --class com.xavient.dataingest.spark.main.SparkIngestion --master spark://<master-ip>:<port> --deploy-mode cluster hdfs://<ip>:<port>/user/hdfs/uber-dip.spark-1.0.0.jar
```

- Once you have sumbmitted the jar , navigate to the Spark UI at http://<spark-ui-server>:<port>/index.html

- The job submitted will look like this:

![alt text](https://github.com/XavientInformationSystems/Data-Ingestion-Platform/blob/master/dataingest-spark/src/main/resources/images/sparkUI.png "Logo Title Text 1") 

- Download the DataIngestUI source code from https://github.com/XavientInformationSystems/DiP-DataIngestionPlatformUI and start it using apache tomcat.


- Open the UI for the application by visiting the URL "http://<tomcat-server>:<port>/DataIngestGUI/UI.jsp" , it will look like this:

![alt text](https://github.com/XavientInformationSystems/Data-Ingestion-Platform/blob/master/dataingest-spark/src/main/resources/images/dataingestUI.png "Logo Title Text 1") 

- Now you have two options, either enter data manually in the message box or upload a file. Below mentioned are some sample data rows:
    - JSON
        - {"id":"XIND10000","author":"Jack White","title":"Hadoop Made Easy","genre":"Programming","price":"50000","publish_date":"2001-09-10","description":"ebook"}
        - {"id":"XIND10002","author":"Steven Berg","title":"Apache Source","genre":"Programming","price":"45000","publish_date":"2001-10-10","description":"ebook"}
    - XML
       -    <catalog><book id="bk101"><author>Gambardella, Matthew</author><title>XML Developer's Guide</title><genre>Computer</genre><price>44.95</price><publish_date>2000-10-01</publish_date><description>An in-depth look at creating applications with XML.</description></book></catalog>
       -    <catalog><book id="bk102"><author>Ralls, Kim</author><title>Midnight Rain</title><genre>Fantasy</genre><price>5.95</price><publish_date>2000-12-16</publish_date><description>A former architect battles corporate zombies,an evil sorceress, and her own childhood to become queen of the world.</description></book></catalog>

-   Press submit after copying the sample data in message box. A pop up will appear which says "message has been published to Kafka"

- Now create an external table in Hive using the below command so as the view the data in zeppelin using below command:
    
```
CREATE EXTERNAL TABLE sparkbooks(String STRING, author STRING, bookName STRING , bookType String , price String , publishedDate String , description String )
COMMENT 'Contains book info'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LOCATION '/user/spark/sparkstreaming/';
```
- Create a view in phoenix by accessing "sqlline.py" to access the data written in Hbase using below command-

```
CREATE VIEW "sparkview"(pid VARCHAR PRIMARY KEY , "books"."author" VARCHAR,"books"."description" VARCHAR ,"books"."genre" VARCHAR ,"books"."id" VARCHAR ,"books"."price" VARCHAR , "books"."publish_date" VARCHAR, "books"."title" VARCHAR )AS SELECT * FROM "sparkbooks";
```
- To use phoenix interpreter in Zeppelin, open the Zeppelin URL at http://<zeppelin-server>:<port>/#/
    - Create a new Phoenix notebook to connect to Phoenix and visualize the data.
        - Type the below command in the interpreter:
```
#Connect phoenix interpreter
%phoenix
#Query the data
select "id","price" from "sparkview"
```

- To use hive interpreter, open the Zeppelin URL at http://<zeppelin-server>:<port>/#/
    -   Create a new Phoenix notebook to connect to Phoenix and visualize the data.
        -   Type the below command in the interpreter:
```
#Connect hive interpreter
%hive
#Query the data
select id,price from sparkbooks;
```

- You can draw various charts/graphs in Zeppelin as shown below:
 
![alt text](https://github.com/XavientInformationSystems/Data-Ingestion-Platform/blob/master/dataingest-spark/src/main/resources/images/zeppelinUI.PNG "Logo Title Text 1") 

#### Extras
- Sample json and xml data can be found in the input folder under src/main/resources.