package com.springboot.springkafkaconsumer.consumer.pojp;

import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class Payment {

    private String paymentId;
    private String transactionId;
    private String cardType;
    private String authorize;
    private String authStatus;
}
