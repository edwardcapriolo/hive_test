package com.jointhegrid.hive_test;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory;
import org.apache.hadoop.hive.ql.session.SessionState;


public abstract class HiveTestEmbedded extends HiveTestBase{

	SessionState ss;
	HiveConf c;
	
	public HiveTestEmbedded() throws IOException {
		super();
	}
	
	public void setUp() throws Exception {
		super.setUp();
		SessionState.initHiveLog4j();
		ss = new SessionState(new HiveConf(HiveTestEmbedded.class));
		SessionState.start(ss);
		c = (HiveConf) ss.getConf();
	}

	public int doHiveCommand(String cmd, Configuration h2conf) {
		int ret = 0;
		String cmd_trimmed = cmd.trim();
		String[] tokens = cmd_trimmed.split("\\s+");
		String cmd_1 = cmd_trimmed.substring(tokens[0].length()).trim();
		CommandProcessor proc = null;

		
		proc = CommandProcessorFactory.get(tokens[0],c);
		
		if (proc instanceof Driver) {
			ret = proc.run(cmd).getResponseCode();
		} else {
			ret = proc.run(cmd_1).getResponseCode();
		}
		return ret;
	}
}
