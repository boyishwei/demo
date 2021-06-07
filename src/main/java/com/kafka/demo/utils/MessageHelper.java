package com.kafka.demo.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.demo.service.KafKaProducerService;
import com.kafka.demo.vo.TradeMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

@Slf4j
public class MessageHelper {

    @Autowired
    ObjectMapper objectMapper;

    public static TradeMessage enrich(TradeMessage tradeMessage){
        BigDecimal quantity = new BigDecimal(tradeMessage.getQuantity());
        BigDecimal price = new BigDecimal(tradeMessage.getPrice());
        BigDecimal amout = quantity.multiply(price).setScale(2, RoundingMode.HALF_UP);
        tradeMessage.setAmount(amout.toPlainString());
        tradeMessage.setReceivedTimeStamp(LocalDateTime.now(ZoneOffset.UTC).toString());

        return tradeMessage;
    }

    public static boolean validate(TradeMessage tradeMessage){
        boolean returnCode = true;
        if(tradeMessage.getTradeReference() == null || tradeMessage.getTradeReference().isEmpty())
            returnCode = false;
        if(tradeMessage.getAccountNumber()== null || tradeMessage.getAccountNumber().isEmpty())
            returnCode = false;
        if(tradeMessage.getStockCode() == null || tradeMessage.getStockCode().isEmpty())
            returnCode = false;
        if(tradeMessage.getQuantity() == null || tradeMessage.getQuantity().isEmpty())
            returnCode = false;
        if(tradeMessage.getPrice() == null || tradeMessage.getPrice().isEmpty())
            returnCode = false;
        if(tradeMessage.getBroker() == null || tradeMessage.getBroker().isEmpty())
            returnCode = false;

        if(returnCode == false) {
            log.error("Broken trade message: " + tradeMessage);
            return false;
        }

        return true;
    }
}
