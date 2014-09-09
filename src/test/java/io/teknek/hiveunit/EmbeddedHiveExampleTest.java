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
import org.apache.hadoop.mapred.JobConf;
import org.junit.Assert;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

public class EmbeddedHiveExampleTest extends HiveTestEmbedded {

    public EmbeddedHiveExampleTest() throws IOException {
        super();
    }

    public void setUp() throws Exception {
        super.setUp();
    }

    public void testEmbeddedHive() throws Exception {
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
        Assert.assertEquals(1, doHiveCommand("create table bla (id int)", c));
        Assert.assertEquals(0, doHiveCommand("select * from bla", c));
        Assert.assertEquals(0, doHiveCommand("select count(1) from bla", c));
        Assert.assertEquals(0, doHiveCommand("select id from bla", c));

    }
}
