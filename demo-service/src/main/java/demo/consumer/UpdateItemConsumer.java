package demo.consumer;

import java.util.concurrent.atomic.AtomicInteger;

import demo.event.UpdateItem;
import demo.mapper.JsonMapper;
import demo.service.ItemService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import static messaging.retry.lib.MessagingRetryHeaders.ORIGINAL_RECEIVED_TIMESTAMP;

@Slf4j
@RequiredArgsConstructor
@Component
public class UpdateItemConsumer {

    final AtomicInteger counter = new AtomicInteger();
    final ItemService itemService;

    @KafkaListener(topics = "update-item", containerFactory = "kafkaListenerContainerFactory")
    public void listen(@Payload final String payload, @Header(KafkaHeaders.RECEIVED_TIMESTAMP) final Long receivedTimestamp, @Header(value = ORIGINAL_RECEIVED_TIMESTAMP, required = false) final Long originalReceivedTimestamp) {
        counter.getAndIncrement();
        log.info("Update Item Consumer: Received message [" +counter.get()+ "] - receivedTimestamp ["+receivedTimestamp+"] - originalReceivedTimestamp ["+originalReceivedTimestamp+"] payload: " + payload);
        try {
            UpdateItem event = JsonMapper.readFromJson(payload, UpdateItem.class);
            itemService.updateItem(event, receivedTimestamp, originalReceivedTimestamp);
        } catch (Exception e) {
            log.error("Error processing message: " + e.getMessage());
        }
    }
}
