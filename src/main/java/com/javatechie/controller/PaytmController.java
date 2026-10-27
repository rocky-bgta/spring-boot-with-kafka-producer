package com.javatechie.controller;

import com.javatechie.dto.PaymentRequest;
import com.javatechie.dto.PaytmRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;
import java.util.UUID;

@RestController
public class PaytmController {

    @Value("${paytm.producer.topic.name}")
    private String topicName;

    @Value("${paytm.producer.payment-batch-size:500}")
    private int paymentBatchSize;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @GetMapping("/publish/{message}")
    public void sendMessage(@PathVariable String message) {
        for (int i = 0; i <= 10000; i++) {
            kafkaTemplate.send(topicName, message + i);
        }
    }

    @PostMapping("/paytm/payment")
    public String doPayment(@RequestBody PaytmRequest<PaymentRequest> paytmRequest) {
        PaymentRequest input = paytmRequest.getPayload();
        for (int i = 1; i <= paymentBatchSize; i++) {
            // Kafka sends are asynchronous: never mutate a payload after handing it to KafkaTemplate.
            PaymentRequest event = new PaymentRequest(
                    UUID.randomUUID().toString(),
                    "SRC_AC" + i,
                    "SRC_DEST" + i,
                    ThreadLocalRandom.current().nextInt(10000),
                    new Date()
            );
            kafkaTemplate.send(topicName, event);
        }
        // Enqueued locally; broker acknowledgements are not awaited by this demo endpoint.
        return "payment events submitted for asynchronous publishing";
    }
}
