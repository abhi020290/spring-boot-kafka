package com.springboot.springkafkaconsumer.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.springboot.springkafkaconsumer.consumer.pojp.Payment;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Component
@Slf4j
public class MessageListener {

    private String KAFKA_CONSUMER_LOGGER = "KAFKA_CONSUMER_LOGGER";

    @KafkaListener(topics = "${consumer.string-topic}",groupId = "string-consumer")
    public void StringListener(String message){
        log.info(KAFKA_CONSUMER_LOGGER+ " "+message);
    }

    @KafkaListener(topics = "${consumer.payment-topic}",groupId = "payment-consumer",containerFactory = "paymentKafkaListenerContainerFactory")
    public void PaymentListener(byte[] payment){
        //Payment payment1 = objectMapper.convertValue(payment, Payment.class);
        String input = new String(payment,StandardCharsets.UTF_8);
        log.info(KAFKA_CONSUMER_LOGGER + " "+ input);
    }

}
