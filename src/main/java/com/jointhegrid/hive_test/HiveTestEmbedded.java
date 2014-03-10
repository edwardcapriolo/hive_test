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

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class HiveTestEmbedded extends HiveTestBase {

    SessionState ss;
    HiveConf c;

    public HiveTestEmbedded() throws IOException {
        super();
    }

    public void setUp() throws Exception {
        super.setUp();
        //SessionState.initHiveLog4j();
        ss = new SessionState(new HiveConf(HiveTestEmbedded.class));
        SessionState.start(ss);
        c = (HiveConf) ss.getConf();
    }

    public int doHiveCommand(String cmd, Configuration h2conf) {
        int ret = 40;
        String cmd_trimmed = cmd.trim();
        String[] tokens = cmd_trimmed.split("\\s+");
        String cmd_1 = cmd_trimmed.substring(tokens[0].length()).trim();
        CommandProcessor proc = null;


        proc = CommandProcessorFactory.get(tokens[0], c);

        ArrayList<String> out = Lists.newArrayList();

        if (proc instanceof Driver) {
            try {

                ret = proc.run(cmd).getResponseCode();
                ((Driver) proc).getResults(out);
            } catch (CommandNeedRetryException ex) {
                Logger.getLogger(HiveTestEmbedded.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            try {
                ret = proc.run(cmd_1).getResponseCode();
            } catch (CommandNeedRetryException ex) {
                Logger.getLogger(HiveTestEmbedded.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return ret;
    }
}
