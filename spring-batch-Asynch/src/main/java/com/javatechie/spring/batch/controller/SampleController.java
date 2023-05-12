package com.javatechie.spring.batch.controller;

import com.javatechie.spring.batch.entity.Customer;
import com.javatechie.spring.batch.repository.CustomerRepository;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@RestController
public class SampleController {


    @Autowired
    private CustomerRepository repository;

    @GetMapping(path = "/insert")
    public boolean startBatch() {

        List<Customer> customerList=getCustomerList();

        repository.saveAll(customerList);
        return  true;
    }


    public List<Customer> getCustomerList()
    {
        List<Customer> customerList=new ArrayList<>();
        String [] arr = {"M", "F"};

        for(int i=1;i<300000;i++) {
            customerList.add(Customer.builder()
                    .age(String.valueOf(Math.floor(Math.random() *(60 - 20 + 1) + 20)))
                    .contact("9666" + i)
                    .email("sajeeth" + i + "@gmail.com")
                    .empId(Long.valueOf(new Random().nextInt(10) + 1))
                    .name("sajeeth"+i)
                    .gender(arr[new Random().nextInt(arr.length)])
                    .build());
        }
        return customerList;

    }

}
