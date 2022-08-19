package com.serendipity.engine.source;

import com.serendipity.engine.utils.ConfigNames;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class KafkaConsumerBuilder {
    private Config config;

    public KafkaConsumerBuilder() {
        config = ConfigFactory.load();
    }

    public FlinkKafkaConsumer<String> build(String topic) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", config.getString(ConfigNames.KAFKA_BOOTSTRAP_SERVERS));
        props.setProperty("auto.offset.reset", config.getString(ConfigNames.KAFKA_OFFSET_AUTORESET));
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), props);
        return kafkaConsumer;
    }
}
