package com.evokly.kafka.connect.mqtt.processors;

import com.evokly.kafka.connect.mqtt.MqttMessageProcessor;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class ValueAsKeyProcessor implements MqttMessageProcessor {
    private static final Logger log = LoggerFactory.getLogger(ValueAsKeyProcessor.class);
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
        //String value = new String(mMessage.getPayload());
        return new SourceRecord[]{new SourceRecord(null, null, kafkaTopic, null,
                Schema.BYTES_SCHEMA, mMessage.getPayload(),
                Schema.BYTES_SCHEMA, null)};
    }
}
