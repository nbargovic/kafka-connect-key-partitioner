package io.confluent.connect.storage.partitioner;

import io.confluent.connect.storage.common.SchemaGenerator;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class KeyPartitioner<T> extends DefaultPartitioner<T> {
    private static final Logger log = LoggerFactory.getLogger(KeyPartitioner.class);
    private static final String KEY_FIELD = "key";
    private static final String SCHEMA_GENERATOR_CLASS =
            "io.confluent.connect.storage.hive.schema.DefaultSchemaGenerator";

    @Override
    public void configure(Map<String, Object> config) {
        this.config = config;
    }

    @Override
    public String encodePartition(SinkRecord sinkRecord) {
        return KEY_FIELD + "=" + String.valueOf(sinkRecord.key());
    }

    @Override
    public String generatePartitionedPath(String topic, String encodedPartition) {
        return topic + delim + encodedPartition;
    }

    @Override
    public List<T> partitionFields() {
        if (partitionFields == null) {
            partitionFields = newSchemaGenerator(config).newPartitionFields(KEY_FIELD);
        }
        return partitionFields;
    }

    @SuppressWarnings("unchecked")
    public SchemaGenerator<T> newSchemaGenerator(Map<String, Object> config) {
        Class<? extends SchemaGenerator<T>> generatorClass = null;
        try {
            generatorClass = getSchemaGeneratorClass();
            return generatorClass.newInstance();
        } catch (ClassNotFoundException
                | ClassCastException
                | IllegalAccessException
                | InstantiationException e) {
            ConfigException ce = new ConfigException("Invalid generator class: " + generatorClass);
            ce.initCause(e);
            throw ce;
        }
    }

    @SuppressWarnings("unchecked")
    protected Class<? extends SchemaGenerator<T>> getSchemaGeneratorClass()
            throws ClassNotFoundException {
        return (Class<? extends SchemaGenerator<T>>) Class.forName(SCHEMA_GENERATOR_CLASS);
    }
}