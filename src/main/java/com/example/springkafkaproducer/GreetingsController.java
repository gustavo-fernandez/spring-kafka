package com.example.springkafkaproducer;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.KafkaTemplate;
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
    var saludo = "Hello " + name;
    var key = UUID.randomUUID().toString();

    Map<String, Object> config = new HashMap<>();
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    var senderOptions = SenderOptions.<String, String>create(config);
    var kafkaSender = KafkaSender.<String, String>create(senderOptions);

    var record = SenderRecord
      .create(new ProducerRecord<>("spring-topic", key, saludo), key);

    return kafkaSender.send(Mono.just(record))
      .next()
      .map(r -> r.correlationMetadata());
  }

}
