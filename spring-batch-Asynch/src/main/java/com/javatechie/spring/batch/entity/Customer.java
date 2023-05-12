package com.javatechie.spring.batch.entity;

import lombok.*;

import javax.persistence.*;

@ToString
@Entity
@Table(name = "CUSTOMER_INFO")
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Customer {
    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    private Long id;

    @Column(name = "NAME")
    private String name;

    @Column(name = "EMAIL")
    private String email;

    @Column(name = "GENDER")
    private String gender;

    @Column(name = "CONTACT")
    private String contact;

    @Column(name = "AGE")
    private String age;

    @Column(name = "employ_id")
    private Long empId;


}
