package messaging.retry.consumer;

import messaging.retry.exception.RetryableMessagingException;
import messaging.retry.handler.RetryHandler;
import messaging.retry.util.TestEventData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class RetryConsumerTest {

    private RetryHandler retryHandlerMock;
    private RetryConsumer consumer;

    @BeforeEach
    public void setUp() {
        retryHandlerMock = mock(RetryHandler.class);
        consumer = new RetryConsumer(retryHandlerMock);
    }

    /**
     * Ensure that the JSON message is successfully passed on to the handler, having been correctly unmarshalled into its PoJO form.
     */
    @Test
    public void testListen_Success() {
        String payload = TestEventData.buildEvent();

        consumer.listen(payload, 1L, 1L, "topic");

        verify(retryHandlerMock, times(1)).handle(payload, 1L, 1L, "topic");
    }

    /**
     * If an exception is thrown, an error is logged but the processing completes successfully.
     *
     * This ensures the consumer offsets are updated so that the message is not redelivered.
     */
    @Test
    public void testListen_ServiceThrowsException() {
        String payload = TestEventData.buildEvent();

        doThrow(new RuntimeException("Service failure")).when(retryHandlerMock).handle(payload, 1L, 1L, "topic");

        consumer.listen(payload, 1L, 1L, "topic");

        verify(retryHandlerMock, times(1)).handle(payload, 1L, 1L, "topic");
    }

    /**
     * If a retryable exception is thrown it is allowed to percolate up.
     *
     * This ensures the consumer offsets are not updated so that the message is redelivered.
     */
    @Test
    public void testListen_ServiceThrowsRetryableMessagingException() {
        String payload = TestEventData.buildEvent();

        doThrow(new RetryableMessagingException("Transient error")).when(retryHandlerMock).handle(payload, 1L, 1L, "topic");

        Exception exception = assertThrows(RetryableMessagingException.class, () -> {
            consumer.listen(payload, 1L, 1L, "topic");
        });
        assertThat(exception.getMessage(), equalTo("Transient error"));
        verify(retryHandlerMock, times(1)).handle(payload, 1L, 1L, "topic");
    }
}
