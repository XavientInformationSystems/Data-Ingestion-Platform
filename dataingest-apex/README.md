# Data Ingestion Platform(DiP) - Apache Apex

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
- Apache Apex 3.4.0
- Apache Hbase 1.1.2.2.4
- Apache Hive 1.2.1.2.4
- Apache Zeppelin 0.6.0.2.4
- Apache Tomcat Server 8.0
- Apache Phoenix 4.4.0.2.4
- Apache Maven
- Java 1.7 or later

#### DiP High Level Process Workflow

![alt text](https://github.com/XavientInformationSystems/Data-Ingestion-Platform/blob/master/dataingest-apex/src/main/resources/images/Architecture.JPG "Application Architecture") 

- Input to the application can be fed from a user interface that allows you either enter data manually or upload the data in XML, JSON file format for bulk processing
- Data Ingested is published by the Kafka broker which streams the data to Kafka Input Operator acts as consumer across the DAG
- Once the message type is identified, the content of the message is extracted and is sent to different Operators for persistence - HBase Operator / HDFS Operator / Console Operator
- Hive external table provides data storage through HDFS and Phoenix provides an SQL interface for HBase tables
- Reporting and visualization  of data is done through Zeppelin

### STEPS

#### Run as root user. Create a group
groupadd -g GID hadoopusers

#### Add user to that group
useradd -g hadoopusers -u UID dtadmin

#### Set password for that user
passwd dtadmin

##### Run as hadoop superuser(hdfs)
hadoop dfs -mkdir -p /user/dtadmin/output

#### Give permission to "sparkstreaming" folder as it required later by user "hive"
hadoop fs -chmod -R 777 /user/dtadmin/output

Install Apache Apex to Hadoop cluster: http://docs.datatorrent.com/installation/

Check Apache Apex UI for verification. Default port: 9090.

- Go to $KAFKA_INSTALL_DIR/bin and create a Kafka topic named "apexTest" using the below mentioned command

./kafka-topics.sh --create --topic apexTest --zookeeper zookeeper-server:port --replication-factor 1 --partition 5

- Download the data ingestion source code from https://github.com/XavientInformationSystems/Data-Ingestion-Platform and compile the code using below commands:

```
### Decompress the zip file.

unzip Data-Ingestion-Platform-master.zip

### Compile and package the source using Maven

cd Data-Ingestion-Platform-master
mvn clean package
```

- Once the code has being successfully compiled, go to the dataingest-apex/target directory and locate Application Package Archive (.apa) file "dip.apex-0.0.1.apa"

- Submit the .apa file to Apache Apex to start your Apex Streaming Application using Apex UI http://<server>:<port 9090-by-default> as shown below:

![alt text](https://github.com/XavientInformationSystems/Data-Ingestion-Platform/blob/master/dataingest-apex/src/main/resources/images/Apex%20UI%20Application%20Upload.JPG "Apex UI Application Upload")

- After .apa file submission the application should appear on Apex UI as below

![alt text](https://github.com/XavientInformationSystems/Data-Ingestion-Platform/blob/master/dataingest-apex/src/main/resources/images/Application%20Submitted.JPG "Application Submitted")

- The Directed Acyclic Graph should look like below:

![alt text](https://github.com/XavientInformationSystems/Data-Ingestion-Platform/blob/master/dataingest-apex/src/main/resources/images/DAG.JPG "Directed Acyclic Graph") 

- Download the DataIngestUI source code from https://github.com/XavientInformationSystems/DiP-DataIngestionPlatformUI and start it using apache tomcat.

- Open the UI for the application by visiting the URL "http://tomcat-server:port/DataIngestGUI/UI.jsp".

![alt text](https://github.com/XavientInformationSystems/Data-Ingestion-Platform/blob/master/dataingest-apex/src/main/resources/images/Data%20Ingest%20UI.jpg "Directed Acyclic Graph")

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
CREATE EXTERNAL TABLE IF NOT EXISTS apexbooks(author STRING, price STRING, genre STRING, description STRING, id STRING, title STRING,   publish_date STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE LOCATION '/user/dtadmin/output';
```
- Create a view in phoenix by accessing "sqlline.py" to access the data written in Hbase using below command-

```
CREATE VIEW "apexbooksview"(pid VARCHAR PRIMARY KEY , "books"."author" VARCHAR,"books"."description" VARCHAR ,"books"."genre" VARCHAR ,"books"."id" VARCHAR ,"books"."price" VARCHAR , "books"."publish_date" VARCHAR, "books"."title" VARCHAR)AS SELECT * FROM "apexbooks";
```
- To use phoenix interpreter in Zeppelin, open the Zeppelin URL at http://zeppelin-server:port/#/
    - Create a new Phoenix notebook to connect to Phoenix and visualize the data.
        - Type the below command in the interpreter:
```
#Connect phoenix interpreter
%phoenix
#Query the data
select "id","price" from "apexbooksview"
```

- To use hive interpreter, open the Zeppelin URL at http://zeppelin-server:port/#/
    -   Create a new Phoenix notebook to connect to Phoenix and visualize the data.
    -   Type the below command in the interpreter:
```
#Connect hive interpreter
%hive

#Query the data
select id,price from apexbooksview;
```

- You can draw various charts/graphs in Zeppelin as shown below:
 
![alt text](https://github.com/XavientInformationSystems/Data-Ingestion-Platform/blob/master/dataingest-apex/src/main/resources/images/Zeppelin%20Report.JPG "Zeppelin Reports")

