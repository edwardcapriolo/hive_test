package com.jointhegrid.hive_test;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;

public class EmbeddedHiveTest extends HiveTestBase {

  public EmbeddedHiveTest() throws IOException {
    super();
  }

  public void testUseEmbedded() throws IOException {
    EmbeddedHive eh = new EmbeddedHive();
    Path p = new Path(this.ROOT_DIR, "bfile");

    FSDataOutputStream o = this.getFileSystem().create(p);
    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(o));
    bw.write("1\n");
    bw.write("2\n");
    bw.close();

    Assert.assertEquals(0, eh.doHiveCommand("create table blax (id int)"));
    Assert.assertEquals(0, eh.doHiveCommand("load data local inpath '" + p.toString() + "' into table blax"));
    Assert.assertEquals(0, eh.doHiveCommand("create table blbx (id int)"));
    Assert.assertEquals(9, eh.doHiveCommand("create table blax (id int)"));
    Assert.assertEquals(0, eh.doHiveCommand("select count(1) from blax"));
  }
}
