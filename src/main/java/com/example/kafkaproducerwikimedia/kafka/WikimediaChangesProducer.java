package com.example.kafkaproducerwikimedia.kafka;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.util.Properties;

@Component
@Slf4j
public class WikimediaChangesProducer {

    private final KafkaProducer<String, String> producer = new KafkaProducer<>(getProducerProperties());

    private final String topic = "wikimedia.recentchange";
    private final String bootstrapServers = "127.0.0.1:9092";

    public WikimediaChangesProducer() {
        log.info("WikimediaChangesProducer started");
        BackgroundEventHandler eventHandler = new WikimediaChangesHandler(producer, topic);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        EventSource.Builder eventSourceBuilder = new EventSource.Builder(URI.create(url));
        BackgroundEventSource.Builder backgroundEventSourceBuilder = new BackgroundEventSource.Builder(eventHandler, eventSourceBuilder);
        BackgroundEventSource eventSource = backgroundEventSourceBuilder.build();

        eventSource.start();
    }

    public Properties getProducerProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("partitioner.class", "org.apache.kafka.clients.producer.RoundRobinPartitioner"); // messages get sent to different partitions in a round-robin manner, alternative: UniformStickyPartitioner

        //set high throughput producer configs
        props.setProperty("linger.ms", "20");
        props.setProperty("batch.size", "32768");
        props.setProperty("compression.type", "snappy");
        return props;
    }
}