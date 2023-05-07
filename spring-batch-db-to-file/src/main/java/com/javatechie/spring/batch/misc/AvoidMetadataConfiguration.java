package com.javatechie.spring.batch.misc;

import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class AvoidMetadataConfiguration extends DefaultBatchConfigurer {


 @Override
 protected JobRepository createJobRepository() throws Exception {
MapJobRepositoryFactoryBean factoryBean = new MapJobRepositoryFactoryBean();
factoryBean.afterPropertiesSet();
 return factoryBean.getObject();
 }
}