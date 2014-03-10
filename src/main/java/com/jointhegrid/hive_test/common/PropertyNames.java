package com.jointhegrid.hive_test.common;

/**
 * Configuration property names;
 */
public enum PropertyNames {
    HIVE_JAR("hive.jar");

    private String propertyName;

    PropertyNames(String propertyName) {
        this.propertyName = propertyName;
    }

    public String toString() {
        return propertyName;
    }
}