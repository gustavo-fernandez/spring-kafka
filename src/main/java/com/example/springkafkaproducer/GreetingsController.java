package com.example.springkafkaproducer;

import com.example.avro.model.Customer;
import com.example.springkafkaproducer.util.AvroSerializer;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;

@RestController
@Slf4j
@RequiredArgsConstructor
public class GreetingsController {

  private final KafkaTemplate<String, String> kafkaTemplate;

  @GetMapping
  public Mono<String> greeting(@RequestParam String name) {
    var customer = new Customer(name, new Random().nextInt(100));
    var key = UUID.randomUUID().toString();

    Map<String, Object> config = new HashMap<>();
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroSerializer.class);

    var senderOptions = SenderOptions.<String, Customer>create(config);
    var kafkaSender = KafkaSender.create(senderOptions);

    var record = SenderRecord
      .create(new ProducerRecord<>("spring-3-topic", key, customer), key);

    return kafkaSender.send(Mono.just(record))
      .next()
      .map(r -> r.correlationMetadata());
  }

}
