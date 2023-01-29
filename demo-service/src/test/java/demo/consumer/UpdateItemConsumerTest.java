package demo.consumer;

import java.util.Map;

import demo.event.UpdateItem;
import demo.mapper.JsonMapper;
import demo.service.ItemService;
import demo.service.ItemStatus;
import demo.util.TestEventData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.messaging.MessageHeaders;

import static java.util.UUID.randomUUID;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class UpdateItemConsumerTest {

    private ItemService serviceMock;
    private UpdateItemConsumer consumer;
    private MessageHeaders headers = new MessageHeaders(Map.of("some-header", "some-value"));

    @BeforeEach
    public void setUp() {
        serviceMock = mock(ItemService.class);
        consumer = new UpdateItemConsumer(serviceMock);
    }

    /**
     * Ensure that the JSON message is successfully passed on to the service, having been correctly unmarshalled into its PoJO form.
     */
    @Test
    public void testListen_Success() {
        UpdateItem testEvent = TestEventData.buildUpdateItemEvent(randomUUID(), ItemStatus.ACTIVE);
        String payload = JsonMapper.writeToJson(testEvent);

        consumer.listen(payload, headers);

        verify(serviceMock, times(1)).updateItem(testEvent, headers);
    }

    /**
     * If an exception is thrown, an error is logged but the processing completes successfully.
     *
     * This ensures the consumer offsets are updated so that the message is not redelivered.
     */
    @Test
    public void testListen_ServiceThrowsException() {
        UpdateItem testEvent = TestEventData.buildUpdateItemEvent(randomUUID(), ItemStatus.ACTIVE);
        String payload = JsonMapper.writeToJson(testEvent);

        doThrow(new RuntimeException("Service failure")).when(serviceMock).updateItem(testEvent, headers);

        consumer.listen(payload, headers);

        verify(serviceMock, times(1)).updateItem(testEvent, headers);
    }
}
