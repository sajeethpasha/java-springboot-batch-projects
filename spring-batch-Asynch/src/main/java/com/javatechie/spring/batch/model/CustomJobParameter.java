package com.javatechie.spring.batch.model;

import org.springframework.batch.core.JobParameter;

import java.io.Serializable;
import java.util.UUID;

public  class CustomJobParameter<T extends Serializable> extends JobParameter {
        private T customParam;
        public CustomJobParameter(T customParam){
            super(UUID.randomUUID().toString());//This is to avoid duplicate JobInstance error
            this.customParam = customParam;
        }
        public T getValue(){
            return customParam;
        }
    }