package demo.integration;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import demo.DemoConfiguration;
import demo.event.CreateItem;
import demo.event.UpdateItem;
import demo.lib.KafkaClient;
import demo.mapper.JsonMapper;
import demo.repository.ItemRepository;
import demo.service.ItemStatus;
import demo.util.TestEventData;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import static java.util.UUID.randomUUID;

@Slf4j
@SpringBootTest(classes = { DemoConfiguration.class } )
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@ActiveProfiles("test")
@EmbeddedKafka(controlledShutdown = true, topics = { "create-item", "update-item-status" })
public class KafkaIntegrationTest {

    final static String CREATE_ITEM_TOPIC = "create-item";
    final static String UPDATE_ITEM_TOPIC = "update-item";

    @Autowired
    private KafkaClient kafkaClient;

    @Autowired
    private ItemRepository itemRepository;

    @Test
    public void testCreateAndUpdateItems() {
        int totalMessages = 5;
        Set<UUID> itemIds = new HashSet<>();

        // Create new items.
        for (int i=0; i<totalMessages; i++) {
            UUID itemId = randomUUID();
            CreateItem createEvent = TestEventData.buildCreateItemEvent(itemId, RandomStringUtils.randomAlphabetic(8));
            kafkaClient.sendMessage(CREATE_ITEM_TOPIC, JsonMapper.writeToJson(createEvent));
            itemIds.add(itemId);
        }

        // Check all messages added to database.
        Awaitility.await().atMost(10, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(() -> itemRepository.findAll().size() == totalMessages);
        // Check all messages have NEW status.
        Awaitility.await().atMost(10, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(() -> itemRepository.findAll().stream().allMatch((item) -> item.getStatus().equals(ItemStatus.NEW.toString())));

        // Update all items.
        itemIds.forEach((itemId) -> {
            UpdateItem updateEvent = TestEventData.buildUpdateItemEvent(itemId, ItemStatus.ACTIVE.toString());
            kafkaClient.sendMessage(UPDATE_ITEM_TOPIC, JsonMapper.writeToJson(updateEvent));
        });
        // Check all messages have transitioned to ACTIVE status.
        Awaitility.await().atMost(10, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(() -> itemRepository.findAll().stream().allMatch((item) -> item.getStatus().equals(ItemStatus.ACTIVE.toString())));
    }
}
