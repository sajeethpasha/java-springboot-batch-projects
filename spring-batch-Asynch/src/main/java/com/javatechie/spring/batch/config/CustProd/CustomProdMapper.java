package com.javatechie.spring.batch.config.CustProd;

import com.javatechie.spring.batch.model.CustomerEmployData;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class CustomProdMapper implements RowMapper<CustomerEmployData>{

    @Override
    public CustomerEmployData mapRow(ResultSet resultSet, int i)
            throws SQLException {
        System.out.println("inside the customer mapper");
        CustomerEmployData customer=new CustomerEmployData();
        customer.setId(resultSet.getLong("ID"));
        customer.setName(resultSet.getString("NAME"));
        customer.setEmail(resultSet.getString("email"));
        customer.setGender(resultSet.getString("GENDER"));
        customer.setContact(resultSet.getString("contact"));
        customer.setAge(resultSet.getString("age"));

        customer.setDesignation(resultSet.getString("designation"));
        customer.setSalary(resultSet.getString("salary"));

        return customer;
    }

}
