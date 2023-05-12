package com.javatechie.spring.batch.config.CustProd;

import com.javatechie.spring.batch.model.CustomerEmployData;
import com.javatechie.spring.batch.repository.CustomerRepository;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

//@Scope(value = "step")
@StepScope
@Component
//@Scope
public class CustProdItemWriter implements ItemWriter<CustomerEmployData> {

    @Autowired
    private CustomerRepository repository;

    private String fileName;

    public CustProdItemWriter(String fileName)
    {
     this.fileName=fileName;
    }



    @Override
    public void write(List<? extends CustomerEmployData> list) throws Exception {

        System.out.println("fileName in writter:"+fileName);

        list.forEach(l->{
            System.out.println(l.toString());
        });
    }

    public void printResultSet(ResultSet resultSet,int lcnt) {
        try {

            System.out.println("****************"+lcnt+"***************");
            ResultSetMetaData rsmd = resultSet.getMetaData();
            int columnsNumber = rsmd.getColumnCount();

//            chunkContext.getStepContext().getStepExecution()
//                    .getJobParameters().getString("ccReportId");
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
