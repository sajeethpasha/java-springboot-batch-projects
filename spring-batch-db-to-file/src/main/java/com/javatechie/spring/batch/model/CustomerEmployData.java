package com.javatechie.spring.batch.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class CustomerEmployData {

    private Long id;
    private String age;
    private String contact;
    private String email;
    private String gender;
    private String name;
    private String designation;
    private String salary;

}
