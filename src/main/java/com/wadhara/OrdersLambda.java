package com.wadhara;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class OrdersLambda {
    private final DynamoDB dynamoDB = new DynamoDB(AmazonDynamoDBClientBuilder.defaultClient());
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final String tableName = "orders";
    public String handleRequest(KinesisEvent event, Context context){
        Table table = dynamoDB.getTable(tableName);
        for(KinesisEvent.KinesisEventRecord record : event.getRecords()){
            String data = StandardCharsets.UTF_8.decode(record.getKinesis().getData()).toString();
            Order order = null;
            try {
                order = objectMapper.readValue(data, Order.class);
            } catch (IOException e) {
                e.printStackTrace();
            }

            Item item = new Item().withPrimaryKey("orderId", order.getOrderId())
                    .withString("product", order.getProduct())
                    .withInt("quantity", order.getQuantity());
            table.putItem(item);
        }

        return "SUCCESS";

    }
}
