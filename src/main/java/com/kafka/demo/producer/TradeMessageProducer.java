package com.kafka.demo.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.demo.handler.JSONFileHandler;
import com.kafka.demo.service.KafKaProducerService;
import com.kafka.demo.utils.MessageHelper;
import com.kafka.demo.vo.TradeMessage;
import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class TradeMessageProducer implements  Runnable {
    @Autowired
    JSONFileHandler jsonFileHandler;
    @Autowired
    KafKaProducerService kafKaProducerService;
    @Autowired
    ObjectMapper objectMapper;



    @SneakyThrows
    @Override
    public void run() {
        while (jsonFileHandler.getInputFile().canRead()){
            if(jsonFileHandler.getInputFile().canRead()){
                List<TradeMessage> messageList = jsonFileHandler.read();
                messageList.stream().filter(MessageHelper::validate).map(MessageHelper::enrich).forEach(kafKaProducerService::sendMessage);
                jsonFileHandler.archive();
            }
            else
                Thread.sleep(10000);
        }

    }

    public static void main(String[] args) {
        new Thread(new TradeMessageProducer()).start();
    }
}
