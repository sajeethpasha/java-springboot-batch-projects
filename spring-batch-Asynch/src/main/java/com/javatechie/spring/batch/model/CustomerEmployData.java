package com.javatechie.spring.batch.model;

import lombok.*;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@ToString
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
