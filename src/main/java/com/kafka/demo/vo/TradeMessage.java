package com.kafka.demo.vo;

import lombok.AllArgsConstructor;
import lombok.Data;
@Data
public class TradeMessage {
   private String tradeReference;
   private String accountNumber;
   private String stockCode;
   private String quantity;
   private String currency;
   private String price;
   private String broker;
   private String amount;
   private String receivedTimeStamp;

}
