package com.example.kafkaproducerwikimedia.kafka;

import com.launchdarkly.eventsource.MessageEvent;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

@Slf4j
public class WikimediaChangesHandler implements BackgroundEventHandler {

    private final KafkaProducer<String, String> producer;
    private final String topic;

    public WikimediaChangesHandler(KafkaProducer<String, String> producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    @Override
    public void onOpen() {
        // no need to do anything when stream opened

    }

    @Override
    public void onClosed() {
        producer.close();
    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent) {
        log.info(messageEvent.getData());
        producer.send(new ProducerRecord<>(topic, messageEvent.getData()));
    }

    @Override
    public void onComment(String s) throws Exception {
        // no need to do anything on comment
    }

    @Override
    public void onError(Throwable throwable) {
        log.error("Error reading stream", throwable);
    }
}