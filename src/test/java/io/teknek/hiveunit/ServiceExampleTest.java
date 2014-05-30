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

import io.teknek.hiveunit.HiveTestService;
import io.teknek.hiveunit.builders.File;
import io.teknek.hiveunit.builders.ResultSet;
import io.teknek.hiveunit.builders.Row;

import java.io.IOException;
import org.apache.hadoop.fs.Path;


public class ServiceExampleTest extends HiveTestService {

  public ServiceExampleTest() throws IOException {
    super();
  }

  public void testExecute() throws Exception {
    Path path = new Path(ROOT_DIR, "afile");
    new File(this.getFileSystem(), path)
      .withRow( new Row().withColumn("1"))
      .withRow( new Row().withColumn("2"))
      .build();
    client.execute("create table  atest  (num int)");
    client.execute("load data local inpath '" + path.toString() + "' into table atest");
    client.execute("select count(1) as cnt from atest");
    assertEquals( new ResultSet()
            .withRow(new Row().withColumn("2")).build(), client.fetchAll());
    client.execute("drop table atest");
  }
}
