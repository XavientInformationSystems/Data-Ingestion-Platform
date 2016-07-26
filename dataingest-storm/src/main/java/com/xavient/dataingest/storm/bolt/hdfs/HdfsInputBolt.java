package com.xavient.dataingest.storm.bolt.hdfs;

import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;

public class HdfsInputBolt {

  public static HdfsBolt getHdfsBolt(float fileSize, String delimiter, String outputPath, String fsURL) {
    RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter(delimiter);
    SyncPolicy syncPolicy = new CountSyncPolicy(1);
    FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(fileSize, Units.MB);
    FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath(outputPath);
    HdfsBolt bolt = new HdfsBolt().withFsUrl(fsURL).withFileNameFormat(fileNameFormat).withRecordFormat(format).withRotationPolicy(rotationPolicy)
        .withSyncPolicy(syncPolicy);
    return bolt;
  }

}
