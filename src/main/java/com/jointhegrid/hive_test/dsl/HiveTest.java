package com.jointhegrid.hive_test.dsl;

import com.jointhegrid.hive_test.EmbeddedHive;
import com.jointhegrid.hive_test.client.EmbeddedHiveClient;
import com.jointhegrid.hive_test.common.Response;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * Provides a utility to make Hive scripts testing easier
 */
public class HiveTest {
    private Hive hive;
    private String scriptFile;
    private Properties hiveClientProperties;
    private Map<String, String> params;

    public HiveTest(String scriptFile, Map<String, String> params) {
        this(new Properties(), scriptFile, params);
    }

    public HiveTest(Properties hiveClientProperties, String scriptFile, Map<String, String> params) {
        this.hiveClientProperties = hiveClientProperties;
        this.scriptFile = scriptFile;
        this.params = params;
    }

    /**
     * Enables the user to get the output for a specific input in
     * order to be able to perform assertions.
     *
     * @param input Map<String, List<String>> variables referring to files to be loaded.
     * @return Response;
     */
    public Response outputForInput(Map<String, List<String>> input) {
        for (String inputKey : input.keySet()) {
            try {
                File file = File.createTempFile(UUID.randomUUID().toString(), ".tmp");
                PrintWriter writer = new PrintWriter(file, "UTF-8");
                for (String line : input.get(inputKey)) {
                    writer.println(line);
                }
                params.put(inputKey, file.getAbsolutePath());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        this.hive = new Hive(new EmbeddedHiveClient(new EmbeddedHive(hiveClientProperties)), scriptFile, params);

        return hive.execute();
    }
}
