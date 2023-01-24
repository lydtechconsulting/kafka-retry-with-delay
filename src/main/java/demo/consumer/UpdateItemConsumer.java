package demo.consumer;

import java.util.concurrent.atomic.AtomicInteger;

import demo.event.UpdateItem;
import demo.mapper.JsonMapper;
import demo.service.ItemService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@RequiredArgsConstructor
@Component
public class UpdateItemConsumer {

    final AtomicInteger counter = new AtomicInteger();
    final ItemService itemService;

    @KafkaListener(topics = "update-item", groupId = "demo-consumer-group", containerFactory = "kafkaListenerContainerFactory")
    public void listen(@Payload final String payload) {
        counter.getAndIncrement();
        log.info("Received message [" +counter.get()+ "] - payload: " + payload);
        try {
            UpdateItem event = JsonMapper.readFromJson(payload, UpdateItem.class);
            itemService.updateItem(event);
        } catch (Exception e) {
            log.error("Error processing message: " + e.getMessage());
        }
    }
}
