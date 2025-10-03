# Kafka Connect source connector for RabbitMQ
kafka-connect-rabbitmq-source is a [Kafka Connect](http://kafka.apache.org/documentation.html#connect) source connector for copying data from RabbitMQ into Apache Kafka.

The connector is supplied as source code which you can easily build into a JAR file.

## Installation

1. Clone the repository and enter the project directory:

```bash
git clone https://github.com/ibm-messaging/kafka-connect-rabbitmq-source.git
cd kafka-connect-rabbitmq-source
```

2. Build the connector using Maven (requires Java 17+):

```bash
mvn clean package
```

The build produces two artifacts under `target/`:

* `kafka-connect-rabbitmq-source-1.0-SNAPSHOT.jar` – the plain connector jar (used for development).
* `kafka-connect-rabbitmq-source-1.0-SNAPSHOT-jar-with-dependencies.jar` – a fat jar that includes the RabbitMQ client library. Use this when copying the connector into a Kafka Connect plugin directory.

3. Provision Kafka, Kafka Connect, and RabbitMQ. The connector works with any Kafka Connect 3.9+ runtime. For a local development environment you can reuse the Kafka tooling distributed with Apache Kafka or Confluent Platform. For Kubernetes deployments see [Running on Strimzi](#running-on-strimzi).

4. Copy the assembled jar (`*-jar-with-dependencies.jar`) into a Kafka Connect plugin directory – for example `/usr/local/share/kafka/plugins/rabbitmq-source/` when running Connect locally.

5. Ensure the plugin directory is referenced by the worker configuration via the `plugin.path` property (comma-separated list of directories). Restart the Kafka Connect worker after adding the connector jar.

## Running in Standalone Mode

Copy the sample worker and connector property files in [`config/`](config/) to your Kafka installation directory and adjust the RabbitMQ host/credentials as required. Then run the following command to start the source connector service in standalone mode:

```bash
connect-standalone connect-standalone.properties rabbitmq-source.properties
```

## Running in Distributed Mode

1. In order to run the connector in distributed mode you must first register the connector with
Kafka Connect service by creating a JSON file in the format below:

```json
{
    "name": "RabbitMQSourceConnector",
    "config": {
        "connector.class": "com.ibm.eventstreams.connect.rabbitmqsource.RabbitMQSourceConnector",
        "tasks.max": "10",
        "kafka.topic" : "kafka_test",
        "rabbitmq.queue" : "rabbitmq_test",
        "rabbitmq.prefetch.count": "500",
        "rabbitmq.automatic.recovery.enabled": "true",
        "rabbitmq.network.recovery.interval.ms": "10000",
        "rabbitmq.topology.recovery.enabled": "true"
    }
}
```

A version of this file, `config/rabbitmq-source.json`, is located in the `config` directory.  To register
the connector do the following:

1. Start the Kafka Connect distributed worker:

```bash
connect-distributed connect-distributed.properties
```

2. Register the connector with the Kafka Connect REST API:

```bash
curl -s -X POST -H 'Content-Type: application/json' --data @config/rabbitmq-source.json http://localhost:8083/connectors
```

You can verify that your connector was properly registered by going to `http://localhost:8083/connectors` which 
should return a full list of available connectors.  This JSON connector profile will be propegated to all workers
across the distributed system.  After following these steps your connector will now run in distributed mode.

## Configuration

Create a target kafka topic named `kafka_test`:

```shell
kafka-topics --create --topic kafka_test --partitions 3 --replication-factor 1 --zookeeper 127.0.0.1:2181
```

Go to the RabbitMQ site at the following URL: `http://localhost:15672/`

Create a new queue `rabbitmq_test`.

## Running on Strimzi

When running on Kubernetes with [Strimzi](https://strimzi.io/), package the connector as a Kafka Connect plugin archive and reference it from a `KafkaConnect` custom resource.

1. Build the connector:

    ```bash
    mvn clean package
    ```

2. Stage the plugin contents using the fat jar produced in the previous step:

    ```bash
    rm -rf build/strimzi
    mkdir -p build/strimzi/rabbitmq-source/lib
    cp target/kafka-connect-rabbitmq-source-1.0-SNAPSHOT-jar-with-dependencies.jar build/strimzi/rabbitmq-source/lib/
    cat <<'EOF' > build/strimzi/rabbitmq-source/manifest.json
    {
      "name": "rabbitmq-source",
      "version": "1.0-SNAPSHOT",
      "description": "RabbitMQ source connector for Kafka Connect",
      "connectors": [
        {
          "name": "RabbitMQSourceConnector",
          "class": "com.ibm.eventstreams.connect.rabbitmqsource.RabbitMQSourceConnector",
          "type": "source"
        }
      ]
    }
    EOF
    ```

3. Create the plugin archive that Strimzi can download:

    ```bash
    (cd build/strimzi && zip -r rabbitmq-source-1.0-SNAPSHOT.zip rabbitmq-source)
    ```

   Host the resulting ZIP file somewhere accessible to the Strimzi build process, for example in an internal artifact repository or an object storage bucket.

4. Reference the ZIP artifact from your `KafkaConnect` resource. The snippet below (also available as [`config/strimzi-kafkaconnect.yaml`](config/strimzi-kafkaconnect.yaml)) extends Strimzi's example by adding the RabbitMQ connector to the build:

    ```yaml
    apiVersion: kafka.strimzi.io/v1beta2
    kind: KafkaConnect
    metadata:
      name: connect
      namespace: playback
      annotations:
        strimzi.io/use-connector-resources: "true"
    spec:
      replicas: 1
      bootstrapServers: kafka-cluster-kafka-bootstrap.playback.svc.cluster.local:9092

      build:
        output:
          type: docker
          image: docker.io/example/kafka-connect-rabbitmq:1.0.0
          pushSecret: dockerhub-credentials
        plugins:
          - name: rabbitmq-source
            artifacts:
              - type: zip
                url: https://artifacts.example.com/rabbitmq-source-1.0-SNAPSHOT.zip

      config:
        group.id: connect-cluster
        offset.storage.topic: connect-offsets
        config.storage.topic: connect-configs
        status.storage.topic: connect-status
        offset.storage.replication.factor: 1
        config.storage.replication.factor: 1
        status.storage.replication.factor: 1
        connector.client.config.override.policy: All
    ```

5. Apply the resource with `kubectl apply -f <file>.yaml`. Strimzi builds a container image that includes the RabbitMQ connector and runs it as part of the Connect cluster. After the pod is ready you can create `KafkaConnector` custom resources (or use the REST API) to deploy specific RabbitMQ source instances.

### TLS / SSL configuration

When connecting to a RabbitMQ broker that requires TLS, enable TLS in the connector configuration and supply the TLS material as needed.

```
rabbitmq.ssl.enabled=true
# Optional – defaults to TLSv1.2
rabbitmq.ssl.algorithm=TLSv1.3

# Truststore used to validate the broker certificate
rabbitmq.ssl.truststore.path=/opt/rabbitmq/client-truststore.p12
rabbitmq.ssl.truststore.password=changeit
rabbitmq.ssl.truststore.type=PKCS12

# Optional keystore when using mutual TLS
rabbitmq.ssl.keystore.path=/opt/rabbitmq/client-keystore.p12
rabbitmq.ssl.keystore.password=changeit
rabbitmq.ssl.keystore.type=PKCS12
```

If TLS is enabled without providing custom trust or key stores, the connector falls back to the JVM defaults. Leaving the keystore properties empty disables client certificate authentication while still using TLS to encrypt the connection.

### Kafka Record Key

When sending message to RabbitMQ, include the key from the Kafka record in the headers as "keyValue". (Only string keys are supported at the moment)
If used in combination with JDBC sink connector, include the ID field name as "keyName" in the headers as well (e.g. "transationId"). The keyName will be used as the primary key when inserting a record.

## Testing

Publish a new item to your RabbitMQ queue `rabbitmq_test` from the http://localhost:15672 webui console. 


Type in the following to view the contents of the `kafka_test` topic on kafka.

```shell
kafka-console-consumer --topic kafka_test --from-beginning --bootstrap-server 127.0.0.1:9092
```

## Issues and contributions
For issues relating specifically to this connector, please use the [GitHub issue tracker](https://github.com/ibm-messaging/kafka-connect-rabbitmq-source/issues). If you do want to submit a Pull Request related to this connector, please read the [contributing guide](CONTRIBUTING.md) first to understand how to sign your commits.


## License
Copyright 2020 IBM Corporation

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    (http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. The project is licensed under the Apache 2 license.
