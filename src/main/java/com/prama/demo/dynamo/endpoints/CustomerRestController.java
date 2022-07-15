package com.prama.demo.dynamo.endpoints;

import com.prama.demo.dynamo.models.BatchGetCustomer;
import com.prama.demo.dynamo.models.Customer;
import com.prama.demo.dynamo.repositories.CustomerRepository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

import static org.springframework.http.HttpStatus.NO_CONTENT;

@RestController
public class CustomerRestController {

    private final CustomerRepository customerRepository;

    public CustomerRestController(CustomerRepository customerRepository) {
        this.customerRepository = customerRepository;
    }

    @GetMapping("/dynamo/customers/{key}")
    public Customer getCustomer(@PathVariable String key) {
        final Customer customer = customerRepository.getCustomer(key);
        System.out.println("******* The description value is " + customer.getCustName());
        return customer;
    }

    @GetMapping("/dyanmo/customers")
    public List<Customer> getCustomers(@RequestParam List<String> keys) {
        return customerRepository.batchGetCustomers(keys);
    }

    @GetMapping("/dyanmo/v1/customers")
    public List<Customer> getCustomers2(@RequestParam List<String> keys) {
        return customerRepository.batchGetCustomers2(keys);
    }

    @PostMapping("/dynamo/customers")
    @ResponseStatus(NO_CONTENT)
    public void registerCustomers(@RequestBody List<Customer> customers) {
        customerRepository.batchRegisterCustomers(customers);
    }

    @PostMapping("/dynamo/customers/generate/{count}")
    @ResponseStatus(NO_CONTENT)
    public void generateTestCustomerData(@PathVariable int count) {
        List<Customer> customers = new ArrayList<>(count + 1);
        for (int i = 0; i < count; i++) {
            customers.add(
                Customer.builder()
                    .id(createBatchId(i))
                    .custName("Name" + i)
                    .email("foo" + i + "@pearson.com")
                    .build());
        }
        customerRepository.batchRegisterCustomers(customers);
    }

    @GetMapping("/dyanmo/customers/generated/{count}")
    public BatchGetCustomer getCustomers(@PathVariable int count) {
        List<String> keys = new ArrayList<>(count + 1);
        for (int i = 0; i < count; i++) {
            keys.add(createBatchId(i));
        }

        final List<Customer> customers = customerRepository.batchGetCustomers3(keys);
        return BatchGetCustomer.builder()
            .customers(customers)
            .requested(count)
            .retrieved(customers.size())
            .build();
    }

    @GetMapping("/dyanmo/v2/customers/generated/{count}")
    public BatchGetCustomer getCustomers2(@PathVariable int count) {
        List<String> keys = new ArrayList<>(count + 1);
        for (int i = 0; i < count; i++) {
            keys.add(createBatchId(i));
        }
        final List<Customer> customers = customerRepository.batchGetCustomers4(keys);
        return BatchGetCustomer.builder()
            .customers(customers)
            .requested(count)
            .retrieved(customers.size())
            .build();
    }

    private String createBatchId(int i) {
        return "batch" + i;
    }

}
