package com.springboot.springkafkaproducer.producer;

import com.springboot.springkafkaproducer.pojo.Payment;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;

@Configuration
public class KafkaProducerConfig {

    @Value("${producer.bootstrap-servers}")
    private String bootStrapServer;

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        HashMap<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String,String> kafkaTemplate(){
        return  new KafkaTemplate<String,String>(producerFactory());
    }

    @Bean
    public ProducerFactory<String, Payment> paymentProducerFactory() {
        HashMap<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String,Payment> paymentKafkaTemplate(){
        return  new KafkaTemplate<String,Payment>(paymentProducerFactory());
    }
}
