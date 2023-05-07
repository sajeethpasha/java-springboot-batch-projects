package com.javatechie.spring.batch.config;

import com.javatechie.spring.batch.entity.Customer;
import com.javatechie.spring.batch.listener.StepSkipListener;
import com.javatechie.spring.batch.listener.UserJobExecutionNotificationListener;
import com.javatechie.spring.batch.listener.UserStepCompleteNotificationListener;
import com.javatechie.spring.batch.misc.CustomerMapper;
import com.javatechie.spring.batch.misc.SampleTasklet;
import com.javatechie.spring.batch.model.CustomerEmployData;
import com.javatechie.spring.batch.repository.CustomerRepository;
import lombok.AllArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.SkipListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.integration.async.AsyncItemProcessor;
import org.springframework.batch.integration.async.AsyncItemWriter;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.sql.DataSource;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

@Configuration
@EnableBatchProcessing
@AllArgsConstructor
public class SpringBatchConfig {

    private JobBuilderFactory jobBuilderFactory;

    private StepBuilderFactory stepBuilderFactory;


    private DataSource dataSource;

    @Bean
    public CustomerProcessor processor() {
        return new CustomerProcessor();
    }


    @Bean
    public JdbcCursorItemReader<CustomerEmployData> jdbcCursorItemReader() {
        String sqlQuery = "SELECT c.id,c.age,c.contact,c.email,c.gender,c.name,e.designation,e.salary FROM batch_db_to_csv.customer_info c  left join batch_db_to_csv.employ_data e  on c.employ_id = e.id";

        System.out.println("jdbcCursorItemReader is called.");
        JdbcCursorItemReader<CustomerEmployData> jdbcCursorItemReader = new JdbcCursorItemReader<>();
        jdbcCursorItemReader.setDataSource(dataSource);
        jdbcCursorItemReader.setSql(sqlQuery);
//        jdbcCursorItemReader.setFetchSize(5000);
        jdbcCursorItemReader.setRowMapper(getCustomerMapper());
        return jdbcCursorItemReader;
    }

//    @Bean
//    public ItemReader<CustomerEmployData> asyncReader(DataSource dataSource) {
//
//        return new JdbcPagingItemReaderBuilder<CustomerEmployData>()
//                .name("Reader")
//                .dataSource(dataSource)
//                .selectClause("SELECT c.id,c.age,c.contact,c.email,c.gender,c.name,e.designation,e.salary FROM batch_db_to_csv.customer_info c  left join batch_db_to_csv.employ_data e  on c.employ_id = e.id")
//                .rowMapper(getCustomerMapper())
//                .build();
//    }


    @Bean
    public CustomerMapper getCustomerMapper() {
        return new CustomerMapper();
    }

    @Bean
    public FlatFileItemWriter<CustomerEmployData> flatFileItemWriter() {
        System.out.println("flatFileItemWriter is called.");
        FlatFileItemWriter<CustomerEmployData> flatFileItemWriter =
                new FlatFileItemWriter<>();

        flatFileItemWriter.setResource(new FileSystemResource("E://data//student"+new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss").format(new Date())+".csv"));
        flatFileItemWriter.setLineAggregator(new DelimitedLineAggregator<CustomerEmployData>() {{
                    setDelimiter(",");
                    setFieldExtractor(new BeanWrapperFieldExtractor<CustomerEmployData>() {{
                        setNames(new String[]{"id", "name", "email", "gender", "contact", "age", "designation", "salary"}); }});

                }});

        flatFileItemWriter.setHeaderCallback
                (writer -> writer.write("Id,Name,email,gender,contact,age,designation,salary"));
        return flatFileItemWriter;
    }


//    @Bean
//    public Step step2() {
//        return  stepBuilderFactory.get("step2")
//                .
//    }

    @Bean
    public Step stepSync() {
        return stepBuilderFactory.get("sync")

                .<CustomerEmployData,CustomerEmployData>chunk(1000)
                .reader(jdbcCursorItemReader())
                .processor(processor())
                .writer(flatFileItemWriter())
//                .taskExecutor(taskExecutor())
                .build();
    }

    @Bean
    public Step stepAsync() {
        return stepBuilderFactory.get("Async")

                .<CustomerEmployData, Future<CustomerEmployData>>chunk(1000)
                .reader(jdbcCursorItemReader())
                .processor(asyncProcessor())
                .writer(asyncWriter())
//                .taskExecutor(taskExecutor())
                .build();
    }
//

    @Bean
    public Job runJob() {
        return jobBuilderFactory.get("importCustomerd")
//                .flow(stepSync())
                .flow(stepAsync())
                .end().build();
    }


    @Bean
    public SkipPolicy skipPolicy() {
        return new ExceptionSkipPolicy();
    }

    @Bean
    public SkipListener skipListener() {
        return new StepSkipListener();
    }


    @Bean
    public TaskExecutor simpleTaskExecutor()
    {
        SimpleAsyncTaskExecutor simpleAsyncTaskExecutor=new SimpleAsyncTaskExecutor();

        simpleAsyncTaskExecutor.setConcurrencyLimit(5);

        return  simpleAsyncTaskExecutor;

    }



    @Bean
    public UserJobExecutionNotificationListener jobExecutionListener() {
        return new UserJobExecutionNotificationListener();
    }

    @Bean
    public UserStepCompleteNotificationListener stepExecutionListener() {
        return new UserStepCompleteNotificationListener();
    }



    @Bean
    public TaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(64);
        executor.setMaxPoolSize(64);
        executor.setQueueCapacity(64);
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.setThreadNamePrefix("MultiThreaded-");
        return executor;
    }

    @Bean
    public AsyncItemProcessor<CustomerEmployData, CustomerEmployData> asyncProcessor() {
        AsyncItemProcessor<CustomerEmployData, CustomerEmployData> asyncItemProcessor = new AsyncItemProcessor<>();
        asyncItemProcessor.setDelegate(processor());
        asyncItemProcessor.setTaskExecutor(taskExecutor());

        return asyncItemProcessor;
    }

    @Bean
    public AsyncItemWriter<CustomerEmployData> asyncWriter() {
        AsyncItemWriter<CustomerEmployData> asyncItemWriter = new AsyncItemWriter<>();
        asyncItemWriter.setDelegate(flatFileItemWriter());
        return asyncItemWriter;
    }


}
