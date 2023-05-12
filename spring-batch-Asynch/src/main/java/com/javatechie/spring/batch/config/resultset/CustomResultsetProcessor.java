package com.javatechie.spring.batch.config.resultset;

import com.javatechie.spring.batch.model.CustomerEmployData;
import org.springframework.batch.item.ItemProcessor;

import java.sql.ResultSet;

//@Component
public class CustomResultsetProcessor implements ItemProcessor<ResultSet, ResultSet> {

    @Override
    public ResultSet process(ResultSet resultSet) {
        System.out.println("inside the process ....");
         return resultSet;
    }
}
