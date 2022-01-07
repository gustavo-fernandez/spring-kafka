package com.example.springkafkaproducer;

import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@Slf4j
@RequiredArgsConstructor
public class GreetingsController {

  private final KafkaTemplate<String, String> kafkaTemplate;

  @GetMapping
  public Mono<Boolean> greeting(@RequestParam String name) {
    var saludo = "Hello " + name;
    var key = UUID.randomUUID().toString();
    kafkaTemplate.send("spring-topic", key, saludo);
    return Mono.just(true);
  }

}
