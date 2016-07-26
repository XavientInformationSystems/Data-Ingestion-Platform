# Data Ingestion Platform(DiP) - Real time data analysis - Spark Streaming

“The previous blog "https://www.linkedin.com/pulse/real-time-data-ingestion-easy-simple-co-dev-neeraj-sabharwal" showed how we can leverage the power of Apache Storm and Kafka to do real time data analytics and visualization.

This blog focusses on using Spark Streaming for performing real time data analysis with the help of Apache Kafka.

Spark Streaming is the next level of the core Spark API that enables high-throughput, flexible, fault-tolerant stream processing of live data streams.

### Spark Streaming Features

  - Micro Batch Model
  - Java, Scala, client bindings
  - Declarative API
  - Very High Throughput 
  - Memory Partitioning
  - Extensive community support

### Spark-Streaming VS Storm
“Storm is a distributed real-time computation system”. Apache Storm is a task parallel continuous computational engine. It defines its workflows in Directed Acyclic Graphs (DAG’s) called “topologies”. These topologies run until shutdown by the user or encountering an unrecoverable failure.

Storm does not natively run on top of typical Hadoop clusters, it uses Apache ZooKeeper and its own master/ minion worker processes to coordinate topologies, master and worker state, and the message guarantee semantics.

Apache Storm is focused on stream processing or what some call complex event processing. Storm implements a fault tolerant method for performing a computation or pipelining multiple computations on an event as it flows into a system. 
One might use Storm to transform unstructured data as it flows into a system into a desired format.

“Apache Spark is a fast and general purpose engine for large-scale data processing”. Workflows are defined in a similar and reminiscent style of MapReduce, however, is much more capable than traditional Hadoop MapReduce. 

Apache Spark has its Streaming API project that allows for continuous processing via short interval batches. Similar to Storm, Spark Streaming jobs run until shutdown by the user or encounter an unrecoverable failure.

Apache Spark does not itself require Hadoop to operate. However, its data parallel paradigm requires a shared filesystem for optimal use of stable data. The stable source can range from S3, NFS, or, more typically, HDFS.

Spark's RDDs are inherently immutable, Spark Streaming implements a method for "batching" incoming updates in user-defined time intervals that get transformed into their own RDDs. 
Spark's parallel operators can then perform computations on these RDDs. This is different from Storm which deals with each event individually.

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

![alt text]("Logo Title Text 1") 

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
hadoop dfs -mkdir /user/spark/sparkstreaming

# Give permission to "sparkstreaming" folder as it required later by user "hive"
hadoop fs -chmod -R 777 /user/spark/sparkstreaming
``` 

- Go to $KAFKA_INSTALL_DIR/bin and create a Kafka topic named "visits" using the below mentioned command
```
./kafka-topics.sh --create --topic visits --zookeeper <zookeeper-server>:<port> --replication-factor 1 --partition 5
```

- Download the source code from <<<TODO>>> and compile the code using below commands:

```
# Decompress the zip file.
unzip spark-kafka.zip

# Compile the code
cd spark-kafka
mvn clean package
```

- Once the code has being successfully compiled, go to the target directory and locate a jar by the name "uber-Spark-Kafka-0.0.1-SNAPSHOT"
- Submit the jar file to spark to start your streaming application using the below command:

```
spark-submit --class com.xavient.spark.streaming.main.SparkIngestion --master spark://10.5.3.166:6066 --deploy-mode cluster hdfs://10.5.3.166:8020/user/hdfs/uber-Spark-Kafka-0.0.1-SNAPSHOT.jar
```

- Once you have sumbmitted the jar , navigate to the Spark UI at http://<spark-ui-server>:<port>/index.html

- The job submitted will look like this:

![alt text](sparkUI.PNG "Logo Title Text 1") 

- Download the DataIngestUI source code from "www.URL" and start it using apache tomcat as shown below:

```
Commands for starting UI application
```

- Open the UI for the application by visiting the URL "http://<tomcat-server>:<port>/DataIngestGUI/UI.jsp" , it will look like this:

![alt text](dataingestUI.PNG "Logo Title Text 1") 

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
 
![alt text](zeppelin.PNG "Logo Title Text 1") 

#### Extras
- Sample json and xml data can be found in the input folder under src/main/resources.