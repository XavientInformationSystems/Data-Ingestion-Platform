package com.xavient.dataingest.apex.operators;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;

public class FileOutputOperator extends AbstractFileOutputOperator<String> {
  
  private static final Charset CS = StandardCharsets.UTF_8;

  private String fileName;

  @Override
  protected String getFileName(String paramINPUT) {
    return fileName;
  }

  @Override
  protected byte[] getBytesForTuple(String paramINPUT) {
    return paramINPUT.toString().getBytes(CS);
  }
  
  public String getFileName() {
    return fileName;
  }

  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

}
