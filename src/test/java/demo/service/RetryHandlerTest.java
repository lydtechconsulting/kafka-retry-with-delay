package demo.service;

import java.time.Instant;
import java.util.Map;

import demo.consumer.MessageHeaders;
import demo.exception.RetryableMessagingException;
import demo.lib.KafkaClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

public class RetryHandlerTest {

    private RetryHandler handler;
    private KafkaClient kafkaClientMock;

    @BeforeEach
    public void setUp() {
        kafkaClientMock = mock(KafkaClient.class);
        final Long retryIntervalSeconds = 10L;
        final Long maxRetryDurationSeconds = 30L;
        handler = new RetryHandler(kafkaClientMock, retryIntervalSeconds, maxRetryDurationSeconds);
    }

    /**
     * Configuration is to retry for up to 30 seconds, with a retry interval of 10 seconds
     *
     * As the original received timestamp of the event (i.e. when it was first created) is 31 seconds ago the event
     * should be discarded.
     */
    @Test
    public void testHandle_shouldDiscard() {
        Long receivedTimestamp = Instant.now().toEpochMilli();
        Long originalReceivedTimestamp = Instant.now().minusSeconds(31).toEpochMilli();
        handler.handle("my-payload", receivedTimestamp, originalReceivedTimestamp, "my-topic");
        verifyNoInteractions(kafkaClientMock);
    }

    /**
     * Configuration is to retry for up to 30 seconds, with a retry interval of 10 seconds.
     *
     * As the original received timestamp of the event (i.e. when it was first created) is 29 seconds ago the event
     * should NOT be discarded.
     *
     * As the event was last retried 11 seconds, it should now be retried by sending to Kafka.
     */
    @Test
    public void testHandle_shouldRetryUpdate() {
        Long receivedTimestamp = Instant.now().minusSeconds(11).toEpochMilli();
        Long originalReceivedTimestamp = Instant.now().minusSeconds(29).toEpochMilli();
        handler.handle("my-payload", receivedTimestamp, originalReceivedTimestamp, "my-topic");
        verify(kafkaClientMock, times(1)).sendMessage("my-topic", "my-payload", Map.of(MessageHeaders.ORIGINAL_RECEIVED_TIMESTAMP, originalReceivedTimestamp));
    }

    /**
     * Configuration is to retry for up to 30 seconds, with a retry interval of 10 seconds.
     *
     * As the original received timestamp of the event (i.e. when it was first created) is 29 seconds ago the event
     * should NOT be discarded.
     *
     * As the event was last retried 9 seconds, it should NOT be retried by sending to Kafka.
     *
     * A RetryableMessagingException should therefore be thrown, ensuring the message is not removed from this retry
     * topic and will be re-polled to check again.  This therefore delays before it is sent back to the original topic
     * for retry.
     */
    @Test
    public void testHandle_shouldDelayRetry() {
        Long receivedTimestamp = Instant.now().minusSeconds(9).toEpochMilli();
        Long originalReceivedTimestamp = Instant.now().minusSeconds(29).toEpochMilli();
        assertThrows(RetryableMessagingException.class, () -> {
            handler.handle("my-payload", receivedTimestamp, originalReceivedTimestamp, "my-topic");
        });
        verifyNoInteractions(kafkaClientMock);
    }
}
