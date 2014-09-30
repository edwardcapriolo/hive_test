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
package io.teknek.hiveunit;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import static io.teknek.hiveunit.common.ResponseStatus.FAILURE;
import static io.teknek.hiveunit.common.ResponseStatus.SUCCESS;


public class EmbeddedHiveTest extends HiveTestBase {

  public EmbeddedHiveTest() throws IOException {
    super();
  }

  public void testUseEmbedded() throws IOException {
    EmbeddedHive eh = new EmbeddedHive(TestingUtil.getDefaultProperties());
    Path p = new Path(this.ROOT_DIR, "bfile");

    FSDataOutputStream o = this.getFileSystem().create(p);
    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(o));
    bw.write("1\n");
    bw.write("2\n");
    bw.close();

    assertEquals(SUCCESS, eh.doHiveCommand("create table blax (id int)").getResponseStatus());
    assertEquals(SUCCESS, eh.doHiveCommand("load data local inpath '" + p.toString() + "' into table blax").getResponseStatus());
    assertEquals(SUCCESS, eh.doHiveCommand("create table blbx (id int)").getResponseStatus());
    assertEquals(FAILURE, eh.doHiveCommand("create table blax (id int)").getResponseStatus());
    assertEquals(SUCCESS, eh.doHiveCommand("select count(1) from blax").getResponseStatus());
  }
}
