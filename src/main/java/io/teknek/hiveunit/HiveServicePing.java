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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.List;

public class HiveServicePing extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
    int ret = ToolRunner.run(new Configuration(), new HiveServicePing(), args);
    System.exit(ret);
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    for (String arg : args) {
      if (arg.contains("=")) {
        String vname = arg.substring(0, arg.indexOf('='));
        String vval = arg.substring(arg.indexOf('=') + 1);
        conf.set(vname, vval.replace("\"", ""));
      }
    }
    System.out.println(conf.get("service.host"));
    System.out.println(conf.get("service.port"));
    ServiceHive sh = new ServiceHive(conf.get("service.host"), conf.getInt("service.port", 10000));
    List<String> tables = sh.client.get_all_tables("default");
    for (String table : tables) {
      System.out.println(table);
    }
    return 0;
  }
}
