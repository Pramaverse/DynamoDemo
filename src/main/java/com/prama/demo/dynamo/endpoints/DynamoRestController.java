package com.prama.demo.dynamo.endpoints;

import com.prama.demo.dynamo.repositories.DynamoDbRepository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class DynamoRestController {

    private DynamoDbRepository dynamoDbRepository;

    public DynamoRestController(DynamoDbRepository dynamoDbRepository) {
        this.dynamoDbRepository = dynamoDbRepository;
    }

    @GetMapping("/dynamo/edges")
    public String getEdges() {
        return dynamoDbRepository.batchGet();
    }

}
