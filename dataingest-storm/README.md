# Data Ingestion Platform(DiP)

Demo YouTube video URL: https://www.youtube.com/watch?v=-QRR6qiFL_U

"Data Ingestion Platform utilizes the true power of the latest edge-cutting technologies in the big data ecosystem to achieve almost real time data analytics and visualization".

DiP, scalable up to thousands of nodes, can take in data from multiple sources and in different forms to store it into multiple platforms and provide you the ability to query the data on the go.

#### DiP Features

  - Multiple Sources
  - Multiple File Formats
  - Easy to use UI
  - Data Persistence
  - Data Visualization

#### DiP Prerequisites 
The API has been tested on below mentioned HDP 2.4 components:
- Apache Hadoop 2.7.1.2.4
- Apache Kafka 0.9.0.2.4	
- Apache Storm 0.10.0.2.4
- Apache Hbase 1.1.2.2.4
- Apache Hive 1.2.1.2.4
- Apache Zeppelin 0.6.0.2.4
- Apache Tomcat Server 8.0
- Apache Phoenix 4.4.0.2.4
- Apache Maven 
- Java 1.7 or later

#### DiP High Level Process Workflow

![alt text](https://github.com/mohnkhan/DiP-DataIngestionPlatform/blob/master/src/main/resources/images/architecture.PNG "Application Architecture") 

- Input to the application can be fed from a user interface that allows you either enter data manually or upload the data in XML, JSON or CSV file format for bulk processing
- Data ingested is published by the Kafka broker which streams the data to Kafka spout which acts as consumer across the topology
- Once the message type is identified, the content of the message is extracted and is sent to different bolts for persistence - HBase bolt or HDFS bolt
- Hive external table provides data storage through HDFS and Phoenix provides an SQL interface for HBase tables
- Reporting and visualization  of data is done through Zeppelin

### STEPS
  - Create a new group and a user in the local systen and map that user to HDFS using below mentioned commands:
```
# Run as root user.
# Create a group
groupadd -g GID hadoopusers

# Add user to that group
useradd -g hadoopusers -u UID storm

# Set password for that user
passwd storm

# Run as hadoop superuser(hdfs)
hdfs dfs -mkdir /user/storm
hdfs dfs -chown -R storm:hadoopusers /user/storm
hdfs dfsadmin -refreshUserToGroupsMappings

# Login as user storm and create a new directory named "output"
su - storm
hadoop dfs -mkdir /user/storm/output
# Give permission to "output" folder as it required later by user "hive"
hdfs dfs -chmod 777 /user/storm/output
``` 

- Go to $KAFKA_INSTALL_DIR/bin and create a Kafka topic named "kafka_topic" using the below mentioned command
```
./kafka-topics.sh --create --topic kafka_topic --zookeeper zookeeper-server:port --replication-factor 1 --partition 5
```

- Download the data ingestion source code from https://github.com/mohnkhan/DiP-DataIngestionPlatform and compile the code using below commands:

```
# Decompress the zip file.
unzip dataingest.zip

# Compile the code
cd dataingest
mvn clean package
```

- Once the code has being successfully compiled, go to the target directory and locate a jar by the name "uber-dataingest-1.0.0.jar"
- Submit the jar file to Storm to start your storm topoloy using below command:

```
storm jar uber-dataingest-1.0.0.jar com.xavient.storm.dataingest.DataIngestionTopology
```

- Once you have sumbmitted the jar , navigate to the Storm UI at http://storm-ui-server:port/index.html

- The topology visualization will look like this:

![alt text](https://github.com/mohnkhan/DiP-DataIngestionPlatform/blob/master/src/main/resources/images/toplogy.PNG "Logo Title Text 1") 

- Download the DataIngestUI source code from https://github.com/mohnkhan/DiP-DataIngestionPlatformUI and start it using apache tomcat as shown below:

```
Commands for starting UI application
```

- Open the UI for the application by visiting the URL "http://tomcat-server:port/DataIngestGUI/UI.jsp" , it will look like this:

![alt text](https://github.com/mohnkhan/DiP-DataIngestionPlatform/blob/master/src/main/resources/images/file-picker.PNG "File Picker") 

- Now you have two options, either enter data manually in the message box or upload a file. Below mentioned are some sample data rows:
    - JSON
        - {"id":"XIND10000","author":"Jack White","title":"Hadoop Made Easy","genre":"Programming","price":"50000","publish_date":"2001-09-10","description":"ebook"}
        - {"id":"XIND10002","author":"Steven Berg","title":"Apache Source","genre":"Programming","price":"45000","publish_date":"2001-10-10","description":"ebook"}
    - XML
       -    ```xml<catalog><book id="bk101"><author>Gambardella, Matthew</author><title>XML Developer's Guide</title><genre>Computer</genre><price>44.95</price><publish_date>2000-10-01</publish_date><description>An in-depth look at creating applications with XML.</description></book></catalog>```
       -    ```xml<catalog><book id="bk102"><author>Ralls, Kim</author><title>Midnight Rain</title><genre>Fantasy</genre><price>5.95</price><publish_date>2000-12-16</publish_date><description>A former architect battles corporate zombies,an evil sorceress, and her own childhood to become queen of the world.</description></book></catalog>```

-   Press submit after copying the sample data in message box. A pop up will appear which says "message has been published to Kafka"

- Now create an external table in Hive using the below command so as the view the data in zeppelin using below command:

```
CREATE EXTERNAL TABLE IF NOT EXISTS books(id STRING, author STRING, title STRING, genre STRING, price STRING, publish_date STRING, description STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE LOCATION '/user/storm/output/';
```
- Create a view in phoenix by accessing "sqlline.py" to access the data written in Hbase using below command-

```
CREATE VIEW "testview"(pid VARCHAR PRIMARY KEY , "books"."author" VARCHAR,"books"."description" VARCHAR ,"books"."genre" VARCHAR ,"books"."id" VARCHAR ,"books"."price" VARCHAR , "books"."publish_date" VARCHAR, "books"."title" VARCHAR )AS SELECT * FROM "books";
```
- To use phoenix interpreter in Zeppelin, open the Zeppelin URL at http://zeppelin-server:port/#/
    - Create a new Phoenix notebook to connect to Phoenix and visualize the data.
        - Type the below command in the interpreter:
```
#Connect phoenix interpreter
%phoenix
#Query the data
select "id","price" from "testview"
```

- To use hive interpreter, open the Zeppelin URL at http://zeppelin-server:port/#/
    -   Create a new Phoenix notebook to connect to Phoenix and visualize the data.
        -   Type the below command in the interpreter:
```
#Connect hive interpreter
%hive
#Query the data
select id,price from books;
```

- You can draw various charts/graphs in Zeppelin as shown below:
 
![alt text](https://github.com/mohnkhan/DiP-DataIngestionPlatform/blob/master/src/main/resources/images/zeppelin-reports.PNG "Reports") 
