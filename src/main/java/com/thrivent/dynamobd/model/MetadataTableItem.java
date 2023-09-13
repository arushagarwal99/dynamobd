package com.thrivent.dynamobd.model;

import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbAttribute;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSortKey;

@DynamoDbBean
public class MetadataTableItem {
    private String partitionKey;

    private String sortKey;

    private String value;

    public MetadataTableItem()
    {

    }
    public MetadataTableItem(String partitionKey, String sortKey, String value)
    {
        this.partitionKey = partitionKey;
        this.sortKey = sortKey;
        this.value = value;
    }



    @DynamoDbAttribute("PK")
    @DynamoDbPartitionKey
    public String getPartitionKey() {
        return partitionKey;
    }

    public void setPartitionKey(String partitionKey) {
        this.partitionKey = partitionKey;
    }
    @DynamoDbAttribute("SK")
    @DynamoDbSortKey
    public String getSortKey() {
        return sortKey;
    }

    public void setSortKey(String sortKey) {
        this.sortKey = sortKey;
    }

    @DynamoDbAttribute( "value")
    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "MetadataTableItem{" +
                "partitionKey='" + partitionKey + '\'' +
                ", sortKey='" + sortKey + '\'' +
                ", value='" + value + '\'' +
                '}';
    }


}
