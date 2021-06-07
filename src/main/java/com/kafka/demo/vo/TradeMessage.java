package com.kafka.demo.vo;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.ToString;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

@Data
@Component
public class TradeMessage {
   @JsonIgnore
   @Autowired
   private ObjectMapper objectMapper;

   private String tradeReference;
   private String accountNumber;
   private String stockCode;
   private String quantity;
   private String currency;
   private String price;
   private String broker;
   private String amount;
   private String receivedTimeStamp;

   public String toString(){
//      return "xxx"; //objectMapper.writeValueAsString(this);
      ObjectMapper objectMapper = new ObjectMapper();
      String message = null;
      try {
         message = objectMapper.writeValueAsString(this);
      } catch (JsonProcessingException e) {
         e.printStackTrace();
      }
      return message;
   }
}
