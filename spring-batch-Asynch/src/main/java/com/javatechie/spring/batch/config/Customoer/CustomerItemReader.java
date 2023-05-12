package com.javatechie.spring.batch.config.Customoer;

import com.javatechie.spring.batch.entity.Customer;
import com.javatechie.spring.batch.repository.CustomerRepository;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.beans.factory.annotation.Autowired;

public class CustomerItemReader  implements ItemReader<Customer> {

    @Autowired
    private CustomerRepository customerRepository;

    @Override
    public Customer read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
         customerRepository.findAll();
        return null;
    }
}
