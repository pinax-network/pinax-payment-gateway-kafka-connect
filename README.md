# kafka-connect-sink-template
This repository contains a template for a Kafka Connect sink connector. The current code just prints every Kafka records to the console. To change it to your own sink you can modify the classes in the `src/main/java/com/pinax/kafka/{console}/connect/sink` directory, and change the *console* occurences to something more specific to your sink.

## Sink Connector
Sink connectors are used to write data from Kafka to external systems. This template provides a basic implementation of a sink connector that writes data to the connector's console.

### Essential Classes
The following classes are essential for the sink connector:
- `ConsoleSinkConfig`: The configuration class that contains the configuration definition of your connector.
```java
public class ConsoleSinkConfig {
        public static final ConfigDef CONFIG_DEF = new ConfigDef()
}
```

- `ConsoleSinkConnector`: The main class that extends `SinkConnector`. This class is responsible for starting the connector and creating the tasks.
```java
public class ConsoleSinkConnector extends SinkConnector {

    protected static final String VERSION = "0.0.1";

    private final Logger logger = LoggerFactory.getLogger(ConsoleSinkConnector.class);

    private Map<String, String> configs = null;

    @Override
    public String version() {
        return VERSION;
    }

    @Override
    public void start(Map<String, String> configMap) {
        configs = configMap;
    }

    @Override
    public ConfigDef config() {
        return ConsoleSinkConfig.CONFIG_DEF;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return ConsoleSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxConfigs) {
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        for (int task = 0; task < maxConfigs; task++) {
            taskConfigs.add(configs);
        }
        return taskConfigs;
    }

    @Override
    public void stop() {}
}

```

- `ConsoleSinkTask`: The task class that extends `SinkTask`. This class is responsible for writing the records to the external system.
```java
public class ConsoleSinkTask extends SinkTask {
    private final Logger logger = LoggerFactory.getLogger(ConsoleSinkConnector.class);

    @Override
    public void start(Map<String, String> properties) {}

    @Override
    public void put(Collection<SinkRecord> records) {}

    @Override
    public void stop() {}

    @Override
    public String version() {
        return ConsoleSinkConnector.VERSION;
    }
}
```

## Package
To build the package, run the following command:
```bash
mvn clean package
```

## Deployment
To deploy the connector, copy the JAR file from the `target` directory to the `plugin.path` directory in the Kafka Connect worker configuration. Then restart the Kafka Connect worker.

**WARNING: You need to use the JAR file that ends with `-jar-with-dependencies.jar`**