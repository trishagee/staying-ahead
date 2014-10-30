package com.mechanitis.talks.stayingahead.answer;

import com.mechanitis.talks.stayingahead.Order;
import com.mongodb.DBCollection;
import org.mongojack.JacksonDBCollection;
import org.mongojack.WriteResult;

import javax.ws.rs.core.Response;

import static java.net.URI.create;
import static javax.ws.rs.core.Response.created;
import static javax.ws.rs.core.Response.serverError;

public class ExampleLambda {
    private DBCollection database;

    public Response saveOrder(final Order order) {
        DBCollection orders = database.getCollection("orders");
        JacksonDBCollection<Order, String> collection = JacksonDBCollection.wrap(orders, Order.class, String.class);

        return save(order, collection,
                    () -> created(create(order.getId())).entity(order).build(),
                    () -> serverError().build());
    }

    private Response save(final Order order, final JacksonDBCollection<Order, String> collection,
                          final SuccessHandler successHandler, final ErrorHandler errorHandler) {
        WriteResult<Order, String> writeResult = collection.save(order);
        if (writeResult == null) {
            return errorHandler.onError();
        } else {
            order.setId(writeResult.getSavedId());
            return successHandler.onSuccess();
        }
    }

    private static interface ErrorHandler {
        Response onError();
    }

    private static interface SuccessHandler {
        Response onSuccess();
    }
}
