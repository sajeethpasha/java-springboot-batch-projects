package com.javatechie.spring.batch.listener;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.batch.core.BatchStatus;


@Slf4j
@RequiredArgsConstructor
public class UserJobExecutionNotificationListener extends JobExecutionListenerSupport {

    @Override
    public void beforeJob(JobExecution jobExecution){
        log.info("UserJobExecutionNotificationListener | beforeJob | Executing job id : " +jobExecution.getJobId());
        super.beforeJob(jobExecution);
    }

    @Override
    public void afterJob(JobExecution jobExecution){

        log.info("UserJobExecutionNotificationListener | afterJob | Executing job id : " +jobExecution.getJobId());

        if(jobExecution.getStatus() == BatchStatus.COMPLETED) {
            log.info("Job Completed");
        }
    }

}
