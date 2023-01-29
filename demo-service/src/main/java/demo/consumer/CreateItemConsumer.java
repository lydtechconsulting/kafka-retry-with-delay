package demo.consumer;

import demo.event.CreateItem;
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
public class CreateItemConsumer {
    final ItemService itemService;

    @KafkaListener(topics = "create-item", containerFactory = "kafkaListenerContainerFactory")
    public void listen(@Payload final String payload) {
        log.info("Create Item Consumer: Received message with payload: " + payload);
        try {
            CreateItem event = JsonMapper.readFromJson(payload, CreateItem.class);
            itemService.createItem(event);
        } catch (Exception e) {
            log.error("Create item - error processing message: " + e.getMessage());
        }
    }
}
