package messaging.retry.handler;

import java.time.Instant;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import messaging.retry.exception.RetryableMessagingException;
import messaging.retry.lib.MessagingRetryHeaders;
import messaging.retry.lib.MessagingRetryKafkaClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class RetryHandler {

    private final MessagingRetryKafkaClient kafkaClient;

    /**
     * The interval that must have passed since the last retry before the event is to be retried again.
     */
    private final Long retryIntervalSeconds;

    /**
     * The maximum amount of time an event should be retried before it should be discarded.
     */
    private final Long maxRetryDurationSeconds;

    public RetryHandler(@Autowired MessagingRetryKafkaClient kafkaClient,
                        @Value("${demo.retry.retryIntervalSeconds}") Long retryIntervalSeconds,
                        @Value("${demo.retry.maxRetryDurationSeconds}") Long maxRetryDurationSeconds) {
        this.kafkaClient = kafkaClient;
        this.retryIntervalSeconds = retryIntervalSeconds;
        this.maxRetryDurationSeconds = maxRetryDurationSeconds;
    }

    public void handle(final String payload, final Long receivedTimestamp, final Long originalReceivedTimestamp, final String originalTopic) {
        if(shouldDiscard(originalReceivedTimestamp)) {
            log.debug("Item {} has exceeded total retry duration - item discarded.", payload);
        } else if(shouldRetryUpdate(receivedTimestamp)) {
            log.debug("Item {} is ready to retry - sending to update-item topic.", payload);
            kafkaClient.sendMessage(originalTopic, payload,
                    Map.of(MessagingRetryHeaders.ORIGINAL_RECEIVED_TIMESTAMP, originalReceivedTimestamp));
        } else {
            log.debug("Item {} is not yet ready to retry on the update-item topic - delaying.", payload);
            throw new RetryableMessagingException("Delaying attempt to retry item "+payload);
        }
    }

    /**
     * Example:
     *
     * Event originally received at 10.00
     *
     * Retry for 5 minutes.
     *
     * Cut off is 10.05
     *
     * If current time is 10.06, then discard.  i.e. current time > (original receipt time + retry duration)
     */
    private boolean shouldDiscard(final Long originalReceivedTimestamp) {
        long cutOffTime = originalReceivedTimestamp + (maxRetryDurationSeconds * 1000);
        return Instant.now().toEpochMilli() > cutOffTime;
    }

    /**
     * Example:
     *
     * Event added to retry topic at 10.10 (this is the received timestamp).
     *
     * Retry interval 2 minutes.
     *
     * Should retry if current time < (received timestamp + retry interval)
     *
     * If current time is 10.11, then delay (by throwing an error so the message is re-polled from the retry topic).
     * i.e. current time < (received timestamp + retry interval) so delay
     *
     * If current time is 10.13, then retry (by sending back to update-item topic).
     * i.e. current time > (receipt time + retry interval) so retry
     */
    private boolean shouldRetryUpdate(final Long receivedTimestamp) {
        long timeForNextRetry = receivedTimestamp + (retryIntervalSeconds * 1000);
        log.debug("retryIntervalSeconds: {} - receivedTimestamp: {} - timeForNextRetry: {} - now: {} - (now > timeForNextRetry): {}", retryIntervalSeconds, receivedTimestamp, timeForNextRetry, Instant.now().toEpochMilli(), Instant.now().toEpochMilli() > timeForNextRetry);
        return Instant.now().toEpochMilli() > timeForNextRetry;
    }
}
