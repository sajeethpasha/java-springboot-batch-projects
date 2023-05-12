package com.javatechie.spring.batch.config.resultset;

import com.javatechie.spring.batch.repository.CustomerRepository;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class CustomItemResultsetWriter implements ItemWriter<ResultSet> {

    @Autowired
    private CustomerRepository repository;

    @Override
    public void write(List<? extends ResultSet> list) throws Exception {
        //System.out.println("Writer Thread "+Thread.currentThread().getName());

        System.out.println();
AtomicInteger lcnt= new AtomicInteger(1);
list.forEach(l->{
    printResultSet(l, lcnt.getAndIncrement());
});

        //list.forEach(f-> System.out.println(f.toString()));
        //repository.saveAll(list);
    }

    public void printResultSet(ResultSet resultSet,int lcnt) {
        try {

            System.out.println("****************"+lcnt+"***************");
            ResultSetMetaData rsmd = resultSet.getMetaData();
            int columnsNumber = rsmd.getColumnCount();

            int cnt=1;
            while (resultSet.next()) {
                System.out.println("count :"+cnt++);
                for (int i = 1; i <= columnsNumber; i++) {
                    if (i > 1) System.out.print(",  ");
                    String columnValue = resultSet.getString(i);
                    System.out.print(columnValue + " " + rsmd.getColumnName(i));
                }

            }

        }
        catch (Exception e)
        {
            System.out.println("Exception:"+e.toString());
        }
    }

}
