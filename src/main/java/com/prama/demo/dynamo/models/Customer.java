package com.prama.demo.dynamo.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;

import java.time.Instant;

/**
 * This class is used by the Enhanced Client examples.
 */
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
@Data
@DynamoDbBean
public class Customer {

    private String id;
    private String custName;
    private String email;
    private Instant registrationDate;

    @DynamoDbPartitionKey
    public String getId() {
        return this.id;
    }
}
