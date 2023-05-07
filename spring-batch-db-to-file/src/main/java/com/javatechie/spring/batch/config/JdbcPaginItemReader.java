package com.javatechie.spring.batch.config;

import com.javatechie.spring.batch.misc.CustomerMapper;
import com.javatechie.spring.batch.misc.IfxSqlPagingQueryProvider;
import com.javatechie.spring.batch.model.CustomerEmployData;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.beans.factory.annotation.Autowired;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

public class JdbcPaginItemReader  {

//    @Autowired
    DataSource ifxDatasource;

//    @Override
    public CustomerEmployData read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        JdbcPagingItemReader<CustomerEmployData> jdbcItemReader = new JdbcPagingItemReader<>();
        jdbcItemReader.setQueryProvider(createQueryProvider());
        jdbcItemReader.setDataSource(ifxDatasource);
        jdbcItemReader.setPageSize(100);
        jdbcItemReader.setRowMapper(new CustomerMapper());
//        return jdbcItemReader;
        return new CustomerEmployData();
    }

    private PagingQueryProvider createQueryProvider() {
        IfxSqlPagingQueryProvider queryProvider = new IfxSqlPagingQueryProvider();
        queryProvider.setSelectClause("SELECT brand, pid, destination, language");
        queryProvider.setFromClause("FROM hotel_property");
        queryProvider.setWhereClause("WHERE language='en'");
        queryProvider.setSortKeys(sortByBrandAsc());
        return queryProvider;
    }

    private Map<String, Order> sortByBrandAsc() {
        Map<String, Order> sortConfiguration = new HashMap<>();
        sortConfiguration.put("brand", Order.ASCENDING);
        sortConfiguration.put("pid", Order.ASCENDING);
        return sortConfiguration;
    }
}
