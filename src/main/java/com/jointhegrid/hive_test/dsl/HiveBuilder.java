package com.jointhegrid.hive_test.dsl;

import com.jointhegrid.hive_test.EmbeddedHive;
import com.jointhegrid.hive_test.ServiceHive;
import com.jointhegrid.hive_test.client.EmbeddedHiveClient;
import com.jointhegrid.hive_test.client.HiveClient;
import com.jointhegrid.hive_test.client.ServiceHiveClient;
import com.jointhegrid.hive_test.common.PropertyNames;

import java.util.Map;
import java.util.Properties;

/**
 * Created by jose.rozanec on 3/13/14.
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

    public HiveBuilder withParams(Map<String, String> params) {
        this.params = params;
        return this;
    }

    public HiveBuilder withClientProperties(Properties clientProperties) {
        this.hiveClientProperties = clientProperties;
        return this;
    }

    public HiveTest hiveTestWithEmbeddedHive(String scriptFile) {
        return new HiveTest(
                createEmbeddedHiveClient(), scriptFile, params
        );
    }

    public HiveTest hiveTestWithServiceHive(String scriptFile) {
        return new HiveTest(
                createServiceHiveClient(), scriptFile, params
        );
    }

    public Hive hiveWithEmbeddedHive(String scriptFile) {
        return new Hive(
                createEmbeddedHiveClient(), scriptFile, params
        );
    }

    public Hive hiveWithServiceHive(String scriptFile) {
        return new Hive(
                createServiceHiveClient(), scriptFile, params
        );
    }

    private HiveClient createEmbeddedHiveClient(){
        if(hiveClientProperties == null){
            hiveClientProperties = new Properties();
        }
        return new EmbeddedHiveClient(new EmbeddedHive(hiveClientProperties));
    }

    private HiveClient createServiceHiveClient(){
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