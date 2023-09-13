package com.thrivent.dynamobd.repository;

import com.thrivent.dynamobd.model.MetadataTableItem;
import org.springframework.beans.factory.annotation.Autowired;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.DeleteItemEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.PageIterable;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;


public class DynamoDbRepository implements DynamoDbRepositoryI {
    @Autowired
    private DynamoDbEnhancedClient dynamoDbenhancedClient ;

    public void save(final MetadataTableItem metadataTableItem) {
        DynamoDbTable<MetadataTableItem> orderTable = getTable();
        orderTable.putItem(metadataTableItem);
    }
    public void deleteData(final String partitionKey, final String sortKey)
    {
        DynamoDbTable<MetadataTableItem> orderTable = getTable();

        Key key = Key.builder()
                .partitionValue(partitionKey)
                .sortValue(sortKey)
                .build();

        DeleteItemEnhancedRequest deleteRequest = DeleteItemEnhancedRequest
                .builder()
                .key(key)
                .build();

        orderTable.deleteItem(deleteRequest);
    }

    // Retrieve a single order item from the database
    public MetadataTableItem getOrder(final String partitionKey, final String sortKey) {
        DynamoDbTable<MetadataTableItem> orderTable = getTable();
        // Construct the key with partition and sort key
        Key key = Key.builder().partitionValue(partitionKey)
                .sortValue(sortKey)
                .build();

        MetadataTableItem order = orderTable.getItem(key);

        return order;
    }
    public PageIterable<MetadataTableItem> findOrdersByValue(final String partitionKey) {
        DynamoDbTable<MetadataTableItem> orderTable = getTable();
        QueryConditional queryConditional = QueryConditional
                .keyEqualTo(Key.builder().partitionValue(partitionKey)
                        .build());
        PageIterable<MetadataTableItem> results =
                orderTable
                        .query(r -> r.queryConditional(queryConditional)
                                );
        return results;
    }

    private DynamoDbTable<MetadataTableItem> getTable() {

        DynamoDbTable<MetadataTableItem> orderTable =
                dynamoDbenhancedClient.table("MetaData",
                        TableSchema.fromBean(MetadataTableItem.class));
        return orderTable;
    }





}
