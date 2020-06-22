package com.ververica.catalog;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.*;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;
import java.util.Set;

public class CodeLoadingConnectorFactory implements Factory, DynamicTableSourceFactory, DynamicTableSinkFactory {

    @Override
    public String factoryIdentifier() {
        return null;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return null;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return null;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        ClassLoader cl = getConnectorClassLoader(context);
        return FactoryUtil.createTableSink(
                null, context.getObjectIdentifier(), context.getCatalogTable(), context.getConfiguration(), cl);
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        ClassLoader cl = getConnectorClassLoader(context);
        return FactoryUtil.createTableSource(
                null, context.getObjectIdentifier(), context.getCatalogTable(), context.getConfiguration(), cl);
    }

    private ClassLoader getConnectorClassLoader(Context ctx) {
        URL[] dependencies = getConnectorDependencies(ctx.getCatalogTable().getOptions());
        return new URLClassLoader(dependencies, ctx.getClassLoader());
    }

    private URL[] getConnectorDependencies(Map<String, String> properties) {

        String connectorType = properties.get("connector");
        String formatType = properties.get("format");

        final URL[] connectorUrls;
        switch (connectorType) {
            case "kafka":
                connectorUrls = new URL[] {
                        getClass().getClassLoader().getResource("flink-sql-connector-kafka_2.11-1.12-SNAPSHOT.jar")
                };
                break;
            default:
                connectorUrls = new URL[] {};
        }

        final URL[] formatUrls;
        switch (formatType) {
            case "json":
                formatUrls = new URL[] {
                        getClass().getClassLoader().getResource("flink-json-1.12-SNAPSHOT-sql-jar.jar")
                };
                break;
            default:
                formatUrls = new URL[] {};
        }
        return ArrayUtils.addAll(connectorUrls, formatUrls);
    }

}
