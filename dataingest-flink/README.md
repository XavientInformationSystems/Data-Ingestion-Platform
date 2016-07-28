# Data Ingestion Platform(DiP) - Real time data analysis - Flink Streaming

“The previous blog "https://www.linkedin.com/pulse/real-time-data-ingestion-easy-simple-co-dev-neeraj-sabharwal" showed how we can leverage the power of Apache Storm and Kafka to do real time data analytics and visualization.

This blog focusses on using Flink Streaming for performing real time data analysis with the help of Apache Kafka.

Flink’s core is a streaming dataflow engine that provides data distribution, communication, and fault tolerance for distributed computations over data streams.

### Flink Streaming Features

  - One Runtime for Streaming and Batch Processing
  - Java, Scala, client bindings
  - Declarative API
  - Very High Throughput 
  - Own memory management implemented inside the JVM.
  - Extensive community support
  - Iterations and Delta Iterations


#### Prerequisites
The API has been tested on below mentioned HDP 2.4 components:
- Apache Hadoop 2.7.1.2.4
- Apache Kafka 0.9.0.2.4	
- Apache Flink 1.0.3
- Apache Hbase 1.1.2.2.4
- Apache Hive 1.2.1.2.4
- Apache Zeppelin 0.6.0.2.4
- Apache Tomcat Server 8.0
- Apache Phoenix 4.4.0.2.4
- Apache Maven 
- Java 1.7 or later

#### High Level Process Workflow with Flink-Streaming

![alt text](https://github.com/XavientInformationSystems/Data-Ingestion-Platform/blob/master/dataingest-flink/src/main/resources/images/FlinkArchitecture.PNG "Logo Title Text 1") 

- Input to the application can be fed from a user interface that allows you either enter data manually or upload the data in XML, JSON or CSV file format for bulk processing
- Data ingested is published by the Kafka broker which streams the data to Kafka consumer process
- Once the message type is identified, the content of the message is extracted from the kafka source  and is sent to different sinks for its persistence 
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
passwd flink

# Run as hadoop superuser(hdfs)
hadoop dfs -mkdir /user/flink/flinkstreaming

# Give permission to "flinkstreaming" folder as it required later by user "hive"
hadoop fs -chmod -R 777 /user/flink/flinkstreaming


``` 


- Go to $KAFKA_INSTALL_DIR/bin and create a Kafka topic named "flinktopic" using the below mentioned command
```
./kafka-topics.sh --create --topic flinktopic --zookeeper <zookeeper-server>:<port> --replication-factor 1 --partition 5
```

- Download the source code from <<<TODO>>> and compile the code using below commands:

```
# Decompress the zip file.
unzip flink-kafka.zip

# Compile the code
cd spark-flink
mvn clean package
```

- Once the code has being successfully compiled, go to the target directory and locate a jar by the name "uber-flink-Kafka-0.0.1-SNAPSHOT"
- Submit the jar file to spark to start your streaming application using the below command:

```
Use the flink dash board to submit the job through UI  or fill in the zookeeper host and port before the maven build and run the process through command line

```

- Once you have sumbmitted the jar , navigate to the Flink UI at http://<spark-ui-server>:<port>/index.html

- The job submitted will look like this:

![alt text](https://github.com/XavientInformationSystems/Data-Ingestion-Platform/blob/master/dataingest-flink/src/main/resources/images/FlinkDashBoard.PNG "Logo Title Text 1") 

- Download the DataIngestUI source code from "www.URL" and start it using apache tomcat as shown below:

```
Commands for starting UI application
```

- Open the UI for the application by visiting the URL "http://<tomcat-server>:<port>/DataIngestGUI/UI.jsp" , it will look like this:

![alt text](https://github.com/XavientInformationSystems/Data-Ingestion-Platform/blob/master/dataingest-flink/src/main/resources/images/DataIngestUI.PNG "Logo Title Text 1") 

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
CREATE EXTERNAL TABLE flinkbooks(String STRING, author STRING, bookName STRING , bookType String , price String , publishedDate String , description String )
COMMENT 'Contains book info'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LOCATION '/user/flink/flinkstreaming/';
```
- Create a view in phoenix by accessing "sqlline.py" to access the data written in Hbase using below command-

```
CREATE VIEW "flinkview"(pid VARCHAR PRIMARY KEY , "books"."author" VARCHAR,"books"."description" VARCHAR ,"books"."genre" VARCHAR ,"books"."id" VARCHAR ,"books"."price" VARCHAR , "books"."publish_date" VARCHAR, "books"."title" VARCHAR )AS SELECT * FROM "flinkbooks";
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
 
![alt text](https://github.com/XavientInformationSystems/Data-Ingestion-Platform/blob/master/dataingest-flink/src/main/resources/images/ZeppelinView.PNG "Logo Title Text 1") 

#### Extras
- Sample json and xml data can be found in the input folder under src/main/resources.