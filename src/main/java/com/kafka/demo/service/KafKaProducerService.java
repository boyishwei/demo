package com.kafka.demo.service;

import com.kafka.demo.constant.AppConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

@Service
@Slf4j
public class KafKaProducerService
{
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public ListenableFuture<SendResult<String, String>> sendMessage(String message)
    {
        log.info(String.format("Message sent -> %s", message));
        return this.kafkaTemplate.send(AppConstants.TOPIC_NAME, message);
    }

}