package io.teknek.hiveunit.common;

/**
 * Configuration property names;
 */
public enum PropertyNames {
    HIVE_JAR("hive.jar"), METASTOREWAREHOUSE("hive.metastore.warehouse.dir"), HOST("host"), PORT("port");

    private String propertyName;

    PropertyNames(String propertyName) {
        this.propertyName = propertyName;
    }

    public String toString() {
        return propertyName;
    }
}
