package com.javatechie.spring.batch.config;

import com.javatechie.spring.batch.entity.Customer;
import com.javatechie.spring.batch.model.CustomerEmployData;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

//@Component
public class CustomerProcessor implements ItemProcessor<CustomerEmployData, CustomerEmployData> {

    @Override
    public CustomerEmployData process(CustomerEmployData customer) {
//        System.out.println("process method called.."+customer.toString());
//        int age = Integer.parseInt(customer.getAge());//vhjkdfh38497infdhg
//        if (age >= 18) {
//            return customer;
//        }
//        if(customer.getId()<3)
//        {
//            return customer;
//        }
        return customer;
    }
}
