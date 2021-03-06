package com.mechanitis.talks.stayingahead.answer;

import com.mechanitis.talks.stayingahead.Order;
import com.mongodb.DBCollection;
import org.mongojack.JacksonDBCollection;
import org.mongojack.WriteResult;

import javax.ws.rs.core.Response;
import java.net.URI;

public class Example {
    private final DBCollection database;

    public Example(final DBCollection database) {
        this.database = database;
    }

    public Response saveOrder(final Order order) {
        DBCollection orders = database.getCollection("orders");
        JacksonDBCollection<Order, String> collection = JacksonDBCollection.wrap(orders, Order.class, String.class);

        WriteResult<Order, String> writeResult = collection.save(order);
        if (writeResult == null) {
            return Response.serverError().build();
        }
        order.setId(writeResult.getSavedId());

        return Response.created(URI.create(order.getId())).entity(order).build();
    }
}
