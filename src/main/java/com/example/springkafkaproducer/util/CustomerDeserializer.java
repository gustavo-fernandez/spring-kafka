package com.example.springkafkaproducer.util;

import com.example.avro.model.Customer;

public class CustomerDeserializer extends AvroDeserializer {

  public CustomerDeserializer() {
    super(Customer.class);
  }
}
