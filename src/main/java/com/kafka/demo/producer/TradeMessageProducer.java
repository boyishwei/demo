package com.kafka.demo.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.demo.handler.JSONFileHandler;
import com.kafka.demo.service.KafKaProducerService;
import com.kafka.demo.utils.MessageHelper;
import com.kafka.demo.vo.TradeMessage;
import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.stream.Collectors;

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
        while (true){
            if(jsonFileHandler.getInputFile().exists()){
                List<TradeMessage> messageList = jsonFileHandler.read();
                List<TradeMessage> enrichedList = messageList.stream().filter(MessageHelper::validate).collect(Collectors.toList());
                enrichedList.forEach(MessageHelper::enrich);
                String messageString = objectMapper.writeValueAsString(enrichedList);
                kafKaProducerService.sendMessage(messageString);
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
