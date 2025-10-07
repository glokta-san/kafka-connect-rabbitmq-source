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
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.*;

import static org.apache.kafka.connect.data.Schema.*;

public class RabbitMQSourceRecordFactory {
    private static final Logger logger = LoggerFactory.getLogger(RabbitMQSourceRecordFactory.class);

    private static final Schema OPTIONAL_ARRAY_OF_STRING = SchemaBuilder.array(STRING_SCHEMA).optional().build();

    private interface HeaderWriter {
        void write(ConnectHeaders headers, String key, Object value);
    }

    private static final LinkedHashMap<Class<?>, HeaderWriter> WRITERS = new LinkedHashMap<>();

    static {
        WRITERS.put(LongString.class, (h, k, v) -> h.addString(k, v.toString()));
        WRITERS.put(String.class, (h, k, v) -> h.addString(k, (String) v));
        WRITERS.put(Integer.class, (h, k, v) -> h.addInt(k, (Integer) v));
        WRITERS.put(Long.class, (h, k, v) -> h.addLong(k, (Long) v));
        WRITERS.put(Short.class, (h, k, v) -> h.addShort(k, (Short) v));
        WRITERS.put(Byte.class, (h, k, v) -> h.addByte(k, (Byte) v));
        WRITERS.put(Boolean.class, (h, k, v) -> h.addBoolean(k, (Boolean) v));
        WRITERS.put(Float.class, (h, k, v) -> h.addFloat(k, (Float) v));
        WRITERS.put(Double.class, (h, k, v) -> h.addDouble(k, (Double) v));
        WRITERS.put(java.sql.Date.class, (h, k, v) -> h.addTimestamp(k, (java.sql.Date) v));
        WRITERS.put(Date.class, (h, k, v) -> h.addTimestamp(k, (Date) v));
        WRITERS.put(byte[].class, (h, k, v) -> h.add(k, v, BYTES_SCHEMA));
    }

    private final RabbitMQSourceConnectorConfig config;
    private final Clock clock = Clock.systemUTC();

    public RabbitMQSourceRecordFactory(RabbitMQSourceConnectorConfig config) {
        this.config = config;
    }

    private HeaderWriter resolveWriter(Object value) {
        for (Map.Entry<Class<?>, HeaderWriter> e : WRITERS.entrySet()) {
            if (e.getKey().isInstance(value)) {
                return e.getValue();
            }
        }
        return null;
    }

    private ConnectHeaders toConnectHeaders(Map<String, Object> ampqHeaders) {
        ConnectHeaders headers = new ConnectHeaders();
        if (ampqHeaders == null || ampqHeaders.isEmpty()) {
            return headers;
        }
        for (Map.Entry<String, Object> kvp : ampqHeaders.entrySet()) {
            String key = kvp.getKey();
            Object value = kvp.getValue();
            if (value == null) {
                headers.add(key, null, OPTIONAL_STRING_SCHEMA);
                continue;
            }
            if (value instanceof List<?> raw) {
                List<String> values = new ArrayList<>(raw.size());
                for (Object o : raw) {
                    values.add(o == null ? null : o.toString());
                }
                headers.addList(key, values, OPTIONAL_ARRAY_OF_STRING);
                continue;
            }
            HeaderWriter writer = resolveWriter(value);
            if (writer != null) {
                writer.write(headers, key, value);
            } else {
                headers.add(key, value.toString(), OPTIONAL_STRING_SCHEMA);
            }
        }
        return headers;
    }

    public SourceRecord makeSourceRecord(String consumerTag, Envelope envelope, AMQP.BasicProperties basicProperties, byte[] bytes) {
        String topic = this.config.kafkaTopic;
        Map<String, ?> sourcePartition = ImmutableMap.of(EnvelopeSchema.FIELD_ROUTINGKEY, envelope.getRoutingKey());
        Map<String, ?> sourceOffset = ImmutableMap.of(EnvelopeSchema.FIELD_DELIVERYTAG, envelope.getDeliveryTag());

        Object rawKey = basicProperties.getHeaders() == null ? null : basicProperties.getHeaders().get(KeySchema.KEY);
        Object key = rawKey == null ? null : rawKey.toString();

        Struct value = ValueSchema.toStruct(consumerTag, envelope, basicProperties, bytes);
        ConnectHeaders headers = basicProperties.getHeaders() == null ? new ConnectHeaders() : toConnectHeaders(basicProperties.getHeaders());

        String messageBody = value.getString(ValueSchema.FIELD_MESSAGE_BODY);
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
