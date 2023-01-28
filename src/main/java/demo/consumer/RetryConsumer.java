package demo.consumer;

import java.util.concurrent.atomic.AtomicInteger;

import demo.exception.RetryableMessagingException;
import demo.service.RetryHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import static demo.consumer.MessageHeaders.ORIGINAL_RECEIVED_TIMESTAMP;
import static demo.consumer.MessageHeaders.ORIGINAL_TOPIC;

@Slf4j
@RequiredArgsConstructor
@Component
public class RetryConsumer {

    final AtomicInteger counter = new AtomicInteger();
    final RetryHandler retryService;

    @KafkaListener(topics = "retry", containerFactory = "kafkaListenerRetryContainerFactory")
    public void listen(@Payload final String payload,
                       @Header(KafkaHeaders.RECEIVED_TIMESTAMP) final Long receivedTimestamp,
                       @Header(value = ORIGINAL_RECEIVED_TIMESTAMP, required = false) final Long originalReceivedTimestamp,
                       @Header(ORIGINAL_TOPIC) final String originalTopic) {
        counter.getAndIncrement();
        log.info("Retry Item Consumer: Received message - receivedTimestamp ["+receivedTimestamp+"] - originalReceivedTimestamp ["+originalReceivedTimestamp+"] payload: " + payload);
        try {
            retryService.handle(payload, receivedTimestamp, originalReceivedTimestamp, originalTopic);
        } catch (RetryableMessagingException e) {
            // Ensure the message is re-polled from this retry topic to be re-evaluated for retrying on the original topic.
            throw e;
        } catch (Exception e) {
            log.error("Error processing message: " + e.getMessage());
        }
    }
}
