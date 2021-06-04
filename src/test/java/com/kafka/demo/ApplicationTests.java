package com.kafka.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.demo.handler.JSONFileHandler;
import com.kafka.demo.service.KafKaConsumerService;
import com.kafka.demo.service.KafKaProducerService;
import com.kafka.demo.utils.MessageHelper;
import com.kafka.demo.vo.TradeMessage;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;

@SpringBootTest
class ApplicationTests {
	@Autowired
	JSONFileHandler jsonFileHandler;
	@Autowired
	KafKaProducerService kafKaProducerService;
	@Autowired
	KafKaConsumerService kafKaConsumerService;
	@Autowired
	ObjectMapper objectMapper;

	@Test
	void should_return_json_objects_after_read_json_file() throws IOException {
		jsonFileHandler.setFileLocation("classpath:data/TradeMessages.json");
		List<TradeMessage> messageList= jsonFileHandler.read();
		assert(messageList.size() != 0);
	}

	@Test
	void should_skip_broken_message() throws IOException {
		jsonFileHandler.setFileLocation("classpath:data/BrokenMessages.json");
		List<TradeMessage> messageList= jsonFileHandler.read();
		assert MessageHelper.validate(messageList.get(0)) == false;
		assert MessageHelper.validate(messageList.get(1)) == false;
		assert MessageHelper.validate(messageList.get(2)) == false;
		assert MessageHelper.validate(messageList.get(3)) == false;
		assert MessageHelper.validate(messageList.get(4)) == true;
		List<TradeMessage> filteredList = messageList.stream().filter(MessageHelper::validate).collect(Collectors.toList());
		assert filteredList.size() == 1;
	}
	@Test
	void should_match_after_enrich_for_amount() throws IOException {
		List<TradeMessage> messageList= jsonFileHandler.read();
		messageList.get(0);
		TradeMessage tradeMessage = MessageHelper.enrich(messageList.get(0));
		assert(tradeMessage != null);
		assert(tradeMessage.getAmount().equals("355701.43"));
		assert(new BigDecimal(tradeMessage.getAmount()).scale() == 2);
		assert(!tradeMessage.getReceivedTimeStamp().isEmpty());
	}

	@Test
	void should_publish_message_to_kafka_queue() throws IOException {
		jsonFileHandler.setFileLocation("classpath:data/TradeMessages.json");
		List<TradeMessage> messageList= jsonFileHandler.read();
        List<TradeMessage> filteredList = messageList.stream().filter(MessageHelper::validate).collect(Collectors.toList());
        filteredList.forEach(MessageHelper::enrich);
		String json = objectMapper.writeValueAsString(filteredList);
		ListenableFuture<SendResult<String, String>> result = kafKaProducerService.sendMessage(json);
		assert result.isDone() == true;
	}
}
