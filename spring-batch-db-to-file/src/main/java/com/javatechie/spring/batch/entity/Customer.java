package com.javatechie.spring.batch.entity;

import lombok.*;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@ToString
@Entity
@Table(name = "CUSTOMER_INFO")
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Customer {
    @Id
    @Column(name = "ID", nullable = false)
    private Long id;

    @Column(name = "NAME")
    private String name;

//    @Column(name = "FIRST_NAME")
//    private String firstName;
//
//    @Column(name = "LAST_NAME")
//    private String lastName;

    @Column(name = "EMAIL")
    private String email;

    @Column(name = "GENDER")
    private String gender;

    @Column(name = "CONTACT")
    private String contact;

    @Column(name = "AGE")
    private String age;

//    @Column(name = "COUNTRY")
//    private String country;

//    @Column(name = "DOB")
//    private String dob;

}
