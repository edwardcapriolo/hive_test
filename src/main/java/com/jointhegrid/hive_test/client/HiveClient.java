package com.jointhegrid.hive_test.client;

import com.jointhegrid.hive_test.common.Response;

/**
 * Wrapper to abstract possible Hive client implementations (ex.: embedded)
 */

public interface HiveClient {

    /**
     * Method for executing commands against Hive
     *
     * @param command - some command
     * @return - Response
     */
    Response execute(String command);

    /**
     * Closes connection to Hive and performs any required cleanup
     */
    public void close();
}
