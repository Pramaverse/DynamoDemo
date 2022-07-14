package com.prama.demo.dynamo.endpoints;

import com.prama.demo.dynamo.models.Customer;
import com.prama.demo.dynamo.repositories.CustomerRepository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

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

}
