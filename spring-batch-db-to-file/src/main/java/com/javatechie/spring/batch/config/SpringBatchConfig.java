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
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
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

@Configuration
@EnableBatchProcessing
@AllArgsConstructor
public class SpringBatchConfig {

    private JobBuilderFactory jobBuilderFactory;

    private StepBuilderFactory stepBuilderFactory;

//    private CustomerRepository customerRepository;

//    private CustomerItemWriter customerItemWriter;

//    private  CustomerItemReader customerItemReader;

    //private  CustomerProcessor customerProcessor;

    private DataSource dataSource;

//    private CustomerMapper customerMapper;

//    @Bean
//    public FlatFileItemReader<Customer> itemReader() {
//        FlatFileItemReader<Customer> flatFileItemReader = new FlatFileItemReader<>();
//        flatFileItemReader.setResource(new FileSystemResource("src/main/resources/customers.csv"));
//        flatFileItemReader.setName("CSV-Reader");
//        flatFileItemReader.setLinesToSkip(1);
//        flatFileItemReader.setLineMapper(lineMapper());
//        return flatFileItemReader;
//    }

    //    private LineMapper<Customer> lineMapper() {
//        DefaultLineMapper<Customer> lineMapper = new DefaultLineMapper<>();
//
//        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
//        lineTokenizer.setDelimiter(",");
//        lineTokenizer.setStrict(false);
//        lineTokenizer.setNames("id", "firstName", "lastName", "email", "gender", "contactNo", "country", "dob", "age");
//
//        BeanWrapperFieldSetMapper<Customer> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
//        fieldSetMapper.setTargetType(Customer.class);
//
//        lineMapper.setLineTokenizer(lineTokenizer);
//        lineMapper.setFieldSetMapper(fieldSetMapper);
//
//        return lineMapper;
//    }
//
    @Bean
    public CustomerProcessor processor() {
        return new CustomerProcessor();
    }

//    @Bean
//    public RepositoryItemWriter<Customer> writer() {
//        RepositoryItemWriter<Customer> writer = new RepositoryItemWriter<>();
//        writer.setRepository(customerRepository);
//        writer.setMethodName("save");
//        return writer;
//    }

    @Bean
    public JdbcCursorItemReader<CustomerEmployData> jdbcCursorItemReader() {
        String sqlQuery = "SELECT c.id,c.age,c.contact,c.email,c.gender,c.name,e.designation,e.salary FROM batch_db_to_csv.customer_info c join batch_db_to_csv.employ_data e on c.id = e.id";

        System.out.println("jdbcCursorItemReader is called.");
        JdbcCursorItemReader<CustomerEmployData> jdbcCursorItemReader = new JdbcCursorItemReader<>();
        jdbcCursorItemReader.setDataSource(dataSource);
        jdbcCursorItemReader.setSql(sqlQuery);
        jdbcCursorItemReader.setRowMapper(getCustomerMapper());
        return jdbcCursorItemReader;
    }


    @Bean
    public CustomerMapper getCustomerMapper() {
        return new CustomerMapper();
    }

    @Bean
    public FlatFileItemWriter<CustomerEmployData> flatFileItemWriter() {
        System.out.println("flatFileItemWriter is called.");
        FlatFileItemWriter<CustomerEmployData> flatFileItemWriter =
                new FlatFileItemWriter<>();

        flatFileItemWriter.setResource(new FileSystemResource("E://data//student.csv"));
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
    public Step step1() {
        return stepBuilderFactory.get("slaveStep")

                .<CustomerEmployData, CustomerEmployData>chunk(3)
                .reader(jdbcCursorItemReader())
                .processor(processor())
                .writer(flatFileItemWriter())
                //.taskExecutor(TaskExecutor())
              //  .taskExecutor(simpleTaskExecutor())
//                .faultTolerant()
//                //.skipLimit(100)
//                //.skip(NumberFormatException.class)
//                //.noSkip(IllegalArgumentException.class)
//                .listener(skipListener())
//                .skipPolicy(skipPolicy())
                .listener(stepExecutionListener())
                .build();
    }

    public Step step2() {
        return stepBuilderFactory.get("tasklet")
                .tasklet(new SampleTasklet())
                .build();
    }
//

    @Bean
    public Job runJob() {
        return jobBuilderFactory.get("importCustomer")
                .flow(step1())
//                .next(step2())
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
    public TaskExecutor TaskExecutor()
    {
        ThreadPoolTaskExecutor threadPoolTaskExecutor=new ThreadPoolTaskExecutor();
        threadPoolTaskExecutor.setCorePoolSize( 1);
        threadPoolTaskExecutor.setMaxPoolSize(1);
        threadPoolTaskExecutor.setQueueCapacity(2);
        threadPoolTaskExecutor.setThreadNamePrefix("Thread N-> :");
        return threadPoolTaskExecutor;
    }

    @Bean
    public UserJobExecutionNotificationListener jobExecutionListener() {
        return new UserJobExecutionNotificationListener();
    }

    @Bean
    public UserStepCompleteNotificationListener stepExecutionListener() {
        return new UserStepCompleteNotificationListener();
    }
}
