package demo.service;

import java.util.Map;
import java.util.Optional;

import demo.consumer.MessageHeaders;
import demo.domain.Item;
import demo.event.CreateItem;
import demo.event.UpdateItem;
import demo.lib.KafkaClient;
import demo.mapper.JsonMapper;
import demo.properties.DemoProperties;
import demo.repository.ItemRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class ItemService {

    @Autowired
    private final ItemRepository itemRepository;

    @Autowired
    private final DemoProperties properties;

    @Autowired
    private final KafkaClient kafkaClient;

    public void createItem(final CreateItem event) {
        Item item = Item.builder()
                .id(event.getId())
                .name(event.getName())
                .status(ItemStatus.NEW)
                .build();
        itemRepository.save(item);
        log.debug("Item persisted to database with Id: {}", event.getId());
    }

    public void updateItem(final UpdateItem event, final Long receivedTimestamp, final Long originalReceivedTimestamp) {
        final Optional<Item> item = itemRepository.findById(event.getId());
        if(item.isPresent()) {
            item.get().setStatus(event.getStatus());
            itemRepository.save(item.get());
            log.debug("Item updated in database with Id: {}", event.getId());
        } else {
            // If the original received timestamp is not set, it is the first time this event has been received.  i.e.
            // it has not yet been retried.  So set the original received timestamp to the received timestamp and pass
            // this as a header on the event.  Also set the topic this event was received as a header so the retry logic
            // knows which topic to send the event back to when it is ready to retry.
            final Long verifiedOriginalReceivedTimestamp = originalReceivedTimestamp != null ? originalReceivedTimestamp : receivedTimestamp;
            log.info("Event Id [" +event.getId()+ "] - verifiedOriginalReceivedTimestamp ["+verifiedOriginalReceivedTimestamp+"]");
            kafkaClient.sendMessage(properties.getTopics().getRetryTopic(), JsonMapper.writeToJson(event),
                    Map.of(MessageHeaders.ORIGINAL_RECEIVED_TIMESTAMP, verifiedOriginalReceivedTimestamp,
                            MessageHeaders.ORIGINAL_TOPIC, properties.getTopics().getItemUpdateTopic()));
        }
    }
}
