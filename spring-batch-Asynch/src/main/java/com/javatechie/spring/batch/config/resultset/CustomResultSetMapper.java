package com.javatechie.spring.batch.config.resultset;

import com.javatechie.spring.batch.model.CustomerEmployData;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class CustomResultSetMapper implements RowMapper<ResultSet>{

    @Override
    public ResultSet mapRow(ResultSet resultSet, int i)
            throws SQLException {
        System.out.println("inside the customer mapper:"+i);


        return resultSet;
    }

}
