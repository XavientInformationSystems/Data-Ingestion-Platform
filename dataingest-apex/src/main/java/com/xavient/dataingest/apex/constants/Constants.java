package com.xavient.dataingest.apex.constants;

public class Constants {

  public static final String KAFKA_TOPIC = "apexTest";
  public static final String FILE_DELIMITER = "|";
  public static final String[] metadataJsonAttributes = { "id", "author", "title", "genre", "price", "publish_date", "description" };
  
  public static final String FILE_NAME = "inputTuples";
  
  //Zookeeper configurations
  public static final int ZK_PORT = 2181;
  public static final String ZK_QUORUM = "10.5.3.166";
  
  // HBase Table properties
  public static final String TABLE_NAME = "twitter";
  public static final String COLUMN_FAMILY = "books";
  public static final String ROW_KEY = "id";
  
}
