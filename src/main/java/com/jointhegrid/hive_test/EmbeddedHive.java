package com.jointhegrid.hive_test;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory;
import org.apache.hadoop.hive.ql.session.SessionState;

public class EmbeddedHive {

  public SessionState ss;
  public HiveConf c;

  public EmbeddedHive() {
    SessionState.initHiveLog4j();
    ss = new SessionState(new HiveConf(EmbeddedHive.class));
    SessionState.start(ss);
    c = (HiveConf) ss.getConf();
  }

  public int doHiveCommand(String cmd) {
    int ret = 0;
    String cmd_trimmed = cmd.trim();
    String[] tokens = cmd_trimmed.split("\\s+");
    String cmd_1 = cmd_trimmed.substring(tokens[0].length()).trim();
    CommandProcessor proc = CommandProcessorFactory.get(tokens[0], c);
    if (proc instanceof Driver) {
      ret = proc.run(cmd).getResponseCode();
    } else {
      ret = proc.run(cmd_1).getResponseCode();
    }
    return ret;
  }
}
