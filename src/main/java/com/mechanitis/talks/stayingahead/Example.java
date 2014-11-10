package com.mechanitis.talks.stayingahead;

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

        return save(order, collection,
                    () -> Response.created(URI.create(order.getId())).entity(order).build(),
                    () -> Response.serverError().build());
    }

    private Response save(final Order order,
                          final JacksonDBCollection<Order, String> collection,
                          final SuccessHandler successHandler, 
                          final ErrorHandler errorHandler) {
        WriteResult<Order, String> writeResult = collection.save(order);
        if (writeResult == null) {
            return errorHandler.onError();
        } else {
            order.setId(writeResult.getSavedId());

            return successHandler.onSuccess();
        }
    }

    @FunctionalInterface
    private interface SuccessHandler {
        Response onSuccess();
    }

    @FunctionalInterface
    private interface ErrorHandler {
        Response onError();
    }
}
