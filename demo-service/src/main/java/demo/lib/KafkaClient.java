package demo.lib;

import java.util.Map;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaClient {

    @Autowired
    private final KafkaTemplate kafkaTemplate;

    public SendResult sendMessage(final String topic, final String data) {
        return this.sendMessage(topic, data, null);
    }

    public SendResult sendMessage(final String topic, final String data, final Map<String, Object> headers) {
        try {
            final MessageBuilder builder = MessageBuilder
                    .withPayload(data)
                    .setHeader(KafkaHeaders.TOPIC, topic);
            if(headers!=null) {
                headers.forEach((key, value) -> builder.setHeader(key, value));
            }
            final Message<String> message = builder.build();
            return (SendResult)kafkaTemplate.send(message).get();
        } catch (Exception e) {
            String message = "Error sending message to topic " + topic;
            log.error(message);
            throw new RuntimeException(message, e);
        }
    }
}
