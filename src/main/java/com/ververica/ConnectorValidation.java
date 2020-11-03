package com.ververica;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.factories.TableFactory;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.LinkedList;
import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;

public class ConnectorValidation {
    public static void main(String[] args) {
        ConnectorValidation validation = new ConnectorValidation();
        URL[] connectorUrls = new URL[]{
                validation.getClass().getClassLoader().getResource("flink-sql-connector-kafka_2.11-1.12-SNAPSHOT.jar")
        };

        URLClassLoader classLoader = new URLClassLoader(connectorUrls);
        List<Factory> factories = discoverFactories(classLoader);
        System.out.println(factories.size());
        for (Factory f : factories) {
            System.out.println(f.factoryIdentifier());
        }
    }

    private static List<Factory> discoverFactories(ClassLoader classLoader) {
        try {
            final List<Factory> result = new LinkedList<>();
            ServiceLoader
                    .load(Factory.class, classLoader)
                    .iterator()
                    .forEachRemaining(result::add);
            return result;
        } catch (ServiceConfigurationError e) {
            //LOG.error("Could not load service provider for factories.", e);
            throw new TableException("Could not load service provider for factories.", e);
        }
    }
}
