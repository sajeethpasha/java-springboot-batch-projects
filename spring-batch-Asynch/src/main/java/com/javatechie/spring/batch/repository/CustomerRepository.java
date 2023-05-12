package com.javatechie.spring.batch.repository;

import com.javatechie.spring.batch.entity.Customer;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Component;

@Component
public interface CustomerRepository extends JpaRepository<Customer,Long> {
}
