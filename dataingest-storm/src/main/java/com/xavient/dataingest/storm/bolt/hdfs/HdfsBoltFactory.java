package com.xavient.dataingest.storm.bolt.hdfs;

import org.apache.storm.hdfs.bolt.HdfsBolt;

import com.xavient.dataingest.storm.constants.Constants;
import com.xavient.dataingest.storm.vo.AppArgs;

public class HdfsBoltFactory {

  public static HdfsBolt getHDFSBolt(AppArgs appArgs) {
    return HdfsInputBolt.getHdfsBolt(Float.parseFloat(appArgs.getProperty(Constants.FILE_SIZE_ROTATION)),
        appArgs.getProperty(Constants.HDFS_OUTPUT_DELIMITER), appArgs.getProperty(Constants.HDFS_OUTPUT_PATH), appArgs.getProperty(Constants.CLUSTER_FS_URL));
  }

}