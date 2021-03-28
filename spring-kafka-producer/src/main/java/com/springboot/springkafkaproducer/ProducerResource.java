package com.springboot.springkafkaproducer;

import com.springboot.springkafkaproducer.pojo.Payment;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@Slf4j
public class ProducerResource {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private KafkaTemplate<String, Payment> paymentKafkaTemplate;

    @Value("${producer.string-topic}")
    private String topic;

    @Value("${producer.payment-topic}")
    private String paymentTopic;

    private String KAFKA_PRODUCER_LOGGER = "KAFKA_PRODUCER_LOGGER";

    @GetMapping("/string/publish")
    public String publishMessage() {
        ListenableFuture<SendResult<String, String>> send = kafkaTemplate.send(topic, UUID.randomUUID().toString(), RandomStringUtils.random(10,true,true));
        send.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
            @Override
            public void onFailure(Throwable throwable) {
                log.error(KAFKA_PRODUCER_LOGGER, "Error while publish message " + throwable.getMessage());
            }

            @Override
            public void onSuccess(SendResult<String, String> stringStringSendResult) {
                log.info(KAFKA_PRODUCER_LOGGER +" "+ stringStringSendResult.toString());
                log.info(KAFKA_PRODUCER_LOGGER +" " + stringStringSendResult.getRecordMetadata().offset());
            }
        });
        return "Successfully Published";
    }

    @GetMapping("/payment/publish")
    public String publishPaymentMessage() {
        ListenableFuture<SendResult<String, Payment>> send = paymentKafkaTemplate.send(paymentTopic, UUID.randomUUID().toString(), generatePayment());
        send.addCallback(new ListenableFutureCallback<SendResult<String, Payment>>() {
            @Override
            public void onFailure(Throwable throwable) {
                log.error(KAFKA_PRODUCER_LOGGER, "Error while publish message " + throwable.getMessage());
            }

            @Override
            public void onSuccess(SendResult<String, Payment> stringStringSendResult) {
                log.info(KAFKA_PRODUCER_LOGGER+ " "+ stringStringSendResult.getRecordMetadata().offset());
                log.info(KAFKA_PRODUCER_LOGGER+ " "+ stringStringSendResult.getRecordMetadata().timestamp());
            }
        });
        return "Successfully Published";
    }

    private Payment generatePayment() {
        Payment payment = new Payment("1232", "shahjsaj243kjsd", "VISA", "Success", "200");
        return payment;
    }

}
