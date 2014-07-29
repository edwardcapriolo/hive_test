package io.teknek.hiveunit.dsl;

import io.teknek.hiveunit.EmbeddedHive;
import io.teknek.hiveunit.ServiceHive;
import io.teknek.hiveunit.client.EmbeddedHiveClient;
import io.teknek.hiveunit.client.HiveClient;
import io.teknek.hiveunit.client.ServiceHiveClient;
import io.teknek.hiveunit.common.PropertyNames;

import java.util.Map;
import java.util.Properties;

/**
 * Builder to create Hive and HiveTest instances
 */
public class HiveBuilder {
    private Properties hiveClientProperties;
    private Map<String, String> params;

    private HiveBuilder() {
        hiveClientProperties = null;
        params = null;
    }

    public static HiveBuilder create() {
        return new HiveBuilder();
    }

    /**
     * Allows to set script execution parameters
     *
     * @param params - Map<String, String>
     * @return same HiveBuilder instance
     */
    public HiveBuilder withParams(Map<String, String> params) {
        this.params = params;
        return this;
    }

    /**
     * Allows to set hive client properties
     *
     * @param clientProperties - Properties
     * @return same HiveBuilder instance
     */
    public HiveBuilder withClientProperties(Properties clientProperties) {
        this.hiveClientProperties = clientProperties;
        return this;
    }

    /**
     * Creates a HiveTest instance using an embedded hive
     *
     * @param scriptFile - String
     * @return new HiveTest instance
     */
    public HiveTest hiveTestWithEmbeddedHive(String scriptFile) {
        return new HiveTest(
                createEmbeddedHiveClient(), scriptFile, params
        );
    }

    /**
     * Creates a HiveTest instance connecting to given client
     *
     * @param scriptFile - String
     * @return new HiveTest instance
     */
    public HiveTest hiveTestWithServiceHive(String scriptFile) {
        return new HiveTest(
                createServiceHiveClient(), scriptFile, params
        );
    }

    /**
     * Creates a Hive instance using an embedded hive
     *
     * @param scriptFile - String
     * @return new Hive instance
     */
    public Hive hiveWithEmbeddedHive(String scriptFile) {
        return new Hive(
                createEmbeddedHiveClient(), scriptFile, params
        );
    }

    /**
     * Creates a Hive instance connecting to given client
     *
     * @param scriptFile - String
     * @return new Hive instance
     */
    public Hive hiveWithServiceHive(String scriptFile) {
        return new Hive(
                createServiceHiveClient(), scriptFile, params
        );
    }

    private HiveClient createEmbeddedHiveClient() {
        if (hiveClientProperties == null) {
            hiveClientProperties = new Properties();
        }
        return new EmbeddedHiveClient(new EmbeddedHive(hiveClientProperties));
    }

    private HiveClient createServiceHiveClient() {
        String host = "localhost";
        if (hiveClientProperties.getProperty(PropertyNames.HOST.toString()) != null) {
            host = hiveClientProperties.getProperty(PropertyNames.HOST.toString());
        }
        int port = 10000;
        if (hiveClientProperties.getProperty(PropertyNames.HOST.toString()) != null) {
            port = Integer.valueOf(hiveClientProperties.getProperty(PropertyNames.PORT.toString()));
        }
        return new ServiceHiveClient(new ServiceHive(host, port));
    }
}