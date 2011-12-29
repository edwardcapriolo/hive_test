/*
Copyright 2011 Edward Capriolo

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
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
