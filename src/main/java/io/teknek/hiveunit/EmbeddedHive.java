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

import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory;
import org.apache.hadoop.hive.ql.session.SessionState;

public class EmbeddedHive {

  public SessionState ss;
  public HiveConf c;

  public EmbeddedHive() {
    //SessionState.initHiveLog4j(); // gone in 0.8.0
    ss = new SessionState(new HiveConf(EmbeddedHive.class));
    SessionState.start(ss);
    c = (HiveConf) ss.getConf();
  }

  public int doHiveCommand(String cmd) throws SQLException {
    int ret = -40;
    String cmd_trimmed = cmd.trim();
    String[] tokens = cmd_trimmed.split("\\s+");
    String cmd_1 = cmd_trimmed.substring(tokens[0].length()).trim();
    CommandProcessor proc = CommandProcessorFactory.get(tokens, c);
    
    if (proc instanceof Driver) {
      try {
        ret = proc.run(cmd).getResponseCode();
      } catch (CommandNeedRetryException ex) {
        Logger.getLogger(EmbeddedHive.class.getName()).log(Level.SEVERE, null, ex);
      }
    } else {
      try {
        ret = proc.run(cmd_1).getResponseCode();
      } catch (CommandNeedRetryException ex) {
        Logger.getLogger(EmbeddedHive.class.getName()).log(Level.SEVERE, null, ex);
      }
    }
    return ret;
  }
}
