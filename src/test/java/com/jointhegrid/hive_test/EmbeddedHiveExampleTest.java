package com.jointhegrid.hive_test;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Assert;

public class EmbeddedHiveExampleTest extends HiveTestEmbedded {

  public EmbeddedHiveExampleTest() throws IOException {
    super();
  }

  public void setUp() throws Exception {
    super.setUp();
  }

  public void testA() throws Exception {
    Path p = new Path(this.ROOT_DIR, "afile");

    FSDataOutputStream o = this.getFileSystem().create(p);
    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(o));
    bw.write("1\n");
    bw.write("2\n");
    bw.close();

    JobConf c = createJobConf();
    Assert.assertEquals(0, doHiveCommand("create table bla (id int)", c));
    Assert.assertEquals(0, doHiveCommand("load data local inpath '" + p.toString() + "' into table bla", c));
    Assert.assertEquals(0, doHiveCommand("create table blb (id int)", c));
    Assert.assertEquals(9, doHiveCommand("create table bla (id int)", c));
    Assert.assertEquals(0, doHiveCommand("select count(1) from bla", c));
  }
}
