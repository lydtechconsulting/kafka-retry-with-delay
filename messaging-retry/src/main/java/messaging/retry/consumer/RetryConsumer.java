package messaging.retry.consumer;

import lombok.extern.slf4j.Slf4j;
import messaging.retry.exception.RetryableMessagingException;
import messaging.retry.service.RetryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import static messaging.retry.lib.MessagingRetryHeaders.ORIGINAL_RECEIVED_TIMESTAMP;
import static messaging.retry.lib.MessagingRetryHeaders.ORIGINAL_RECEIVED_TOPIC;

@Slf4j
@Component
public class RetryConsumer {

    private final RetryService retryHandler;
    public final String messagingRetryTopic;

    public RetryConsumer(@Autowired RetryService retryHandler,
                         @Value("${retry.messaging.topic}") String messagingRetryTopic) {
        this.retryHandler = retryHandler;
        this.messagingRetryTopic = messagingRetryTopic;
    }

    @KafkaListener(topics = "#{retryConsumer.messagingRetryTopic}", containerFactory = "kafkaListenerRetryContainerFactory")
    public void listen(@Payload final String payload,
                       @Header(KafkaHeaders.RECEIVED_TIMESTAMP) final Long receivedTimestamp,
                       @Header(value = ORIGINAL_RECEIVED_TIMESTAMP, required = false) final Long originalReceivedTimestamp,
                       @Header(ORIGINAL_RECEIVED_TOPIC) final String originalTopic) {
        log.info("Retry Item Consumer: Received message - receivedTimestamp ["+receivedTimestamp+"] - originalReceivedTimestamp ["+originalReceivedTimestamp+"] payload: " + payload);
        try {
            retryHandler.handle(payload, receivedTimestamp, originalReceivedTimestamp, originalTopic);
        } catch (RetryableMessagingException e) {
            // Ensure the message is re-polled from this retry topic to be re-evaluated for retrying on the original topic.
            throw e;
        } catch (Exception e) {
            log.error("Retry event - error processing message: " + e.getMessage());
        }
    }
}
