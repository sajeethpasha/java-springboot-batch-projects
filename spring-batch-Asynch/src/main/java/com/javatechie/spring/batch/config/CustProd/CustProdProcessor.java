package com.javatechie.spring.batch.config.CustProd;

import com.javatechie.spring.batch.model.CustomerEmployData;
import org.springframework.batch.item.ItemProcessor;

import java.sql.ResultSet;

//@Component
public class CustProdProcessor implements ItemProcessor<CustomerEmployData, CustomerEmployData> {

    @Override
    public CustomerEmployData process(CustomerEmployData customerEmployData) {
        System.out.println("inside the process ....");
         return customerEmployData;
    }
}
