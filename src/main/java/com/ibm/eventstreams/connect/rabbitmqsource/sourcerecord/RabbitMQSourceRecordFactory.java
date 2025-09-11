package com.ibm.eventstreams.connect.rabbitmqsource.sourcerecord;

import com.google.common.collect.ImmutableMap;
import com.ibm.eventstreams.connect.rabbitmqsource.config.RabbitMQSourceConnectorConfig;
import com.ibm.eventstreams.connect.rabbitmqsource.schema.EnvelopeSchema;
import com.ibm.eventstreams.connect.rabbitmqsource.schema.KeySchema;
import com.ibm.eventstreams.connect.rabbitmqsource.schema.ValueSchema;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.LongString;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.*;

import static org.apache.kafka.connect.data.Schema.*;

public class RabbitMQSourceRecordFactory {
    private static final Logger logger = LoggerFactory.getLogger(RabbitMQSourceRecordFactory.class);

    private final RabbitMQSourceConnectorConfig config;
    private final Clock clock = Clock.systemUTC();

    public RabbitMQSourceRecordFactory(RabbitMQSourceConnectorConfig config) {
        this.config = config;
    }

    private ConnectHeaders toConnectHeaders(Map<String, Object> ampqHeaders) {
        ConnectHeaders headers = new ConnectHeaders();
        if (ampqHeaders == null || ampqHeaders.isEmpty()) {
            return headers;
        }

        for (Map.Entry<String, Object> kvp : ampqHeaders.entrySet()) {
            final String key = kvp.getKey();
            Object value = kvp.getValue();

            // Normalize RabbitMQ-specific header container types
            if (value instanceof LongString) {
                // store as string
                headers.addString(key, value.toString());
                continue;
            }
            if (value instanceof List) {
                @SuppressWarnings("unchecked")
                final List<LongString> list = (List<LongString>) value;
                final List<String> values = new ArrayList<>(list.size());
                for (LongString l : list) {
                    values.add(l.toString());
                }
                // add a list-of-strings header with an explicit array schema for forward-compat
                final Schema arrayOfString = SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build();
                headers.addList(key, values, arrayOfString);
                continue;
            }

            // Use typed adders when possible; otherwise fall back to generic with schema last
            if (value == null) {
                headers.add(key, null, OPTIONAL_STRING_SCHEMA);
            } else if (value instanceof String) {
                headers.addString(key, (String) value);
            } else if (value instanceof Integer) {
                headers.addInt(key, (Integer) value);
            } else if (value instanceof Long) {
                headers.addLong(key, (Long) value);
            } else if (value instanceof Short) {
                headers.addShort(key, (Short) value);
            } else if (value instanceof Byte) {
                headers.addByte(key, (Byte) value);
            } else if (value instanceof Boolean) {
                headers.addBoolean(key, (Boolean) value);
            } else if (value instanceof Float) {
                headers.addFloat(key, (Float) value);
            } else if (value instanceof Double) {
                headers.addDouble(key, (Double) value);
            } else if (value instanceof Date) {
                headers.addTimestamp(key, (Date) value);
            } else {
                // generic fallback as string to avoid classloader/type issues across Connect versions
                headers.add(key, value.toString(), OPTIONAL_STRING_SCHEMA);
            }
        }

        return headers;
    }

    public SourceRecord makeSourceRecord(String consumerTag, Envelope envelope, AMQP.BasicProperties basicProperties, byte[] bytes) {
        final String topic = this.config.kafkaTopic;
        final Map<String, ?> sourcePartition = ImmutableMap.of(EnvelopeSchema.FIELD_ROUTINGKEY, envelope.getRoutingKey());
        final Map<String, ?> sourceOffset = ImmutableMap.of(EnvelopeSchema.FIELD_DELIVERYTAG, envelope.getDeliveryTag());

        Object key = null;
        if (basicProperties.getHeaders() != null){
            key = basicProperties.getHeaders().get(KeySchema.KEY);
        }
        key = key == null ? null : key.toString();
        final Struct value = ValueSchema.toStruct(consumerTag, envelope, basicProperties, bytes);

        ConnectHeaders headers = new ConnectHeaders();
        if (basicProperties.getHeaders() != null) {
            headers = toConnectHeaders(basicProperties.getHeaders());
        }
        final String messageBody = value.getString(ValueSchema.FIELD_MESSAGE_BODY);
        long timestamp = Optional.ofNullable(basicProperties.getTimestamp()).map(Date::getTime).orElse(clock.millis());

        return new SourceRecord(
                sourcePartition,
                sourceOffset,
                topic,
                null,
                OPTIONAL_STRING_SCHEMA,
                key,
                STRING_SCHEMA,
                messageBody,
                timestamp,
                headers
        );
    }
}
