package com.ververica;

import com.ververica.catalog.ConnectorFactoryEnabledCatalog;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class Query {

    private static final boolean DYNAMIC_CONNECTOR_LOADING_ENABLED = true;

    public static void main(String[] args) {

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        TableEnvironment tEnv = TableEnvironment.create(settings);

        ConnectorFactoryEnabledCatalog cat =
                new ConnectorFactoryEnabledCatalog("cat", DYNAMIC_CONNECTOR_LOADING_ENABLED);
        tEnv.registerCatalog("cat", cat);
        tEnv.useCatalog("cat");

        // create custom source and sink tables
        tEnv.executeSql(
                "CREATE TABLE src (" +
                "  id INT, " +
                "  name VARCHAR" +
                ") WITH (" +
                "  'connector' = 'kafka', " +
                "  'properties.bootstrap.servers' = 'localhost:9092', " +
                "  'topic' = 'src'," +
                "  'format' = 'json'" +
                ")");

        tEnv.executeSql(
                "CREATE TABLE snk (" +
                "  id INT, " +
                "  name VARCHAR" +
                ") WITH (" +
                "  'connector' = 'kafka', " +
                "  'properties.bootstrap.servers' = 'localhost:9092', " +
                "  'topic' = 'src'," +
                "  'format' = 'json'" +
                ")");

        // run query
        System.out.println(tEnv.explainSql("INSERT INTO snk SELECT * FROM src"));

    }
}
