package demo.util;

import java.util.UUID;

import demo.domain.Item;
import demo.event.CreateItem;
import demo.event.UpdateItem;
import demo.service.ItemStatus;

public class TestEntityData {

    public static Item buildItem(UUID id, String name) {
        return Item.builder()
                .id(id)
                .name(name)
                .status(ItemStatus.ACTIVE.toString())
                .build();
    }
}
