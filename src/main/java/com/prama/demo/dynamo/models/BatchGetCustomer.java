package com.prama.demo.dynamo.models;

import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder(toBuilder = true)
public class BatchGetCustomer {
    private int requested;
    private int retrieved;
    private List<Customer> customers;

}
