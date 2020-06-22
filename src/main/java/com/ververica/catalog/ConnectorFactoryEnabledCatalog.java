package com.ververica.catalog;

import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.factories.Factory;

import java.util.Optional;

public class ConnectorFactoryEnabledCatalog extends GenericInMemoryCatalog {

    private final boolean dynamicConnectors;

    public ConnectorFactoryEnabledCatalog(String name, boolean dynamicConnectors) {
        super(name);
        this.dynamicConnectors = dynamicConnectors;
    }

    @Override
    public Optional<Factory> getFactory() {
        if (dynamicConnectors) {
            return Optional.of(new CodeLoadingConnectorFactory());
        } else {
            return Optional.empty();
        }
    }

}
