# kafka-processor-dot-net

KafkaDocker directory is a copy of https://github.com/wurstmeister/kafka-docker, but docker-compose-single-broker.yml is modified to start [Kafka web UI](https://github.com/tchiotludo/kafkahq) along with a Kafka broker. The steps to run:

- execute in root
```
docker-compose -f KafkaDocker\docker-compose-single-broker.yml up
```
Kafka broker will be accessible internally at `kafka:9091` (for web UI) and externally via `localhost:9092` (for producers and consumers). Web UI is accessible at `localhost:8080`. Test topic `test-topic` is created (3 partitions, 1 replica).

- start producers/consumers. Configure rate of producing/consuming via `_randomRange` field.

### Existing issues

At the moment bus queue size doesn't affect anything and messages are pushed in the queue even if limit is reached.