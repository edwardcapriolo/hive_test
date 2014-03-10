package com.jointhegrid.hive_test.client;

import com.jointhegrid.hive_test.EmbeddedHive;
import com.jointhegrid.hive_test.common.Response;
import org.apache.log4j.Logger;

/**
 * HiveClient implementation that allows to execute statements against an embedded Hive
 */
public class EmbeddedHiveClient implements HiveClient {
    private static final Logger log = Logger.getLogger(EmbeddedHiveClient.class);
    private EmbeddedHive hive;

    public EmbeddedHiveClient(EmbeddedHive hive) {
        this.hive = hive;
    }

    @Override
    public Response execute(String command) {
        log.info("Will execute command " + command);
        return hive.doHiveCommand(command);
    }
}
