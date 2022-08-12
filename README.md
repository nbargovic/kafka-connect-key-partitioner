# kafka-connect-key-partitioner
Custom Connect storage partitioner that will use the Kafka message key to partition data.  When used with the SFTP Sink Connector, all messages with the same key will land in the same directory.

For example the output directory and file name will be: `<prefix>/topics/<topic-name>/key=<msg-key>/<topic-name>+<kafkaPartition>+<startOffset>.<format>`

### To Build:

- Requires JDK 1.8 to build the jar (example: java-1.8.0-openjdk-devel)
- Run: `./gradlew clean jar`
- Jar file will be generated in `./build/libs/kafka-connect-key-partitioner-1.0.0.jar`

### To Deploy:
https://docs.confluent.io/kafka-connect-sftp/current/sink-connector/index.html#partitioning-records-into-sftp-files