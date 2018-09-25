package com.evokly.kafka.connect.mqtt.processors;

import com.evokly.kafka.connect.mqtt.MqttMessageProcessor;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Copyright 2016 Evokly S.A.
 *
 * <p>See LICENSE file for License
 **/
public class DumbProcessor implements MqttMessageProcessor {
    private static final Logger log = LoggerFactory.getLogger(DumbProcessor.class);
    private MqttMessage mMessage;
    private String mqttTopic;

    @Override
    public MqttMessageProcessor process(String mqttTopic, MqttMessage message) {
        log.debug("processing data for topic: {}; with message {}", mqttTopic, message);
        this.mqttTopic = mqttTopic;
        this.mMessage = message;
        return this;
    }

    @Override
    public String getOriginalTopic() {
        return mqttTopic;
    }


    @Override
    public SourceRecord[] getRecords(String kafkaTopic) {
        return new SourceRecord[]{new SourceRecord(null, null, kafkaTopic, null,
                Schema.STRING_SCHEMA, mqttTopic,
                Schema.STRING_SCHEMA, new String(mMessage.getPayload()))};
    }
}
