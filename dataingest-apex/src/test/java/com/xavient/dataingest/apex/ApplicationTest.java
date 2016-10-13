package com.xavient.dataingest.apex;

import java.io.IOException;

import javax.validation.ConstraintViolationException;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.api.LocalMode;
import com.xavient.dip.apex.ApexTwitterStreamProcessor;

public class ApplicationTest {

  @Test
  public void testApplication() throws IOException, Exception {
    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties.xml"));
      lma.prepareDAG(new ApexTwitterStreamProcessor(), conf);
      //LocalMode.runApp(new ApexStreamProcessor(), 1000000);
      LocalMode.Controller lc = lma.getController();
      //lc.runAsync();
      lc.run(30000); // runs for 60 seconds and quits
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }
}
