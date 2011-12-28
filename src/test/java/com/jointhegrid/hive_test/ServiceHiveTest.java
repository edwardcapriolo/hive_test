package com.jointhegrid.hive_test;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

public class ServiceHiveTest extends HiveTestService {

  public ServiceHiveTest() throws IOException {
    super();
  }

  public void testExecute() throws Exception {
    Path p = new Path(this.ROOT_DIR, "afile");

    FSDataOutputStream o = this.getFileSystem().create(p);
    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(o));
    bw.write("1\n");
    bw.write("2\n");
    bw.close();
    ServiceHive sh = new ServiceHive();

    sh.client.execute("create table  atest  (num int)");
    sh.client.execute("load data local inpath '" + p.toString() + "' into table atest");
    sh.client.execute("select count(1) as cnt from atest");
    String row = sh.client.fetchOne();
    assertEquals("2", row);
    sh.client.execute("drop table atest");
  }
}
