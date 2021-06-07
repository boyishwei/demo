package com.kafka.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.scala.util.StringW;
import com.kafka.demo.constant.AppConstants;
import com.kafka.demo.handler.JSONFileHandler;
import com.kafka.demo.service.KafKaConsumerService;
import com.kafka.demo.service.KafKaProducerService;
import com.kafka.demo.utils.MessageHelper;
import com.kafka.demo.vo.TradeMessage;
import lombok.extern.slf4j.Slf4j;
import lombok.var;
import org.json.JSONArray;
import org.json.JSONException;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.ComponentScans;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.util.concurrent.ListenableFuture;
import org.testcontainers.shaded.org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

@Slf4j
@EmbeddedKafka(topics = "trade",count = 4,ports = {9092,9093,9094,9095})
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
	void should_successfully_send_and_receive_message_via_kafka() throws InterruptedException {
		kafKaProducerService.sendMessage("test message");
		kafKaConsumerService.getLatch().await(10000, TimeUnit.MILLISECONDS);
		assert(kafKaConsumerService.getLatch().getCount()==0L);
		assertThat(kafKaConsumerService.getPayload(), containsString("test message"));
	}

	@Test
	void should_return_json_objects_after_read_json_file() throws IOException {
//		jsonFileHandler.setFileLocation("classpath:data/TradeMessages.json");
		jsonFileHandler.setFileLocation("classpath:data/output_file_one.json");
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
    void should_write_message_to_json_file() throws IOException, JSONException {
		File file = new File("src/main/resources/data/output_file_one.json");
		if(file.exists())
			file.delete();
		else
			file.createNewFile();


		jsonFileHandler.setFileLocation("classpath:data/TradeMessages.json");
		List<TradeMessage> messageList= jsonFileHandler.read();
		String firstJSON = messageList.get(0).toString() ;

		jsonFileHandler.writeToJsonFile(file,messageList.get(0).toString());

		String json = FileUtils.readFileToString(file, "UTF-8");
		JSONAssert.assertEquals("[" + firstJSON + "]", json, true);


		jsonFileHandler.writeToJsonFile(file, messageList.get(1).toString());
		json = FileUtils.readFileToString(file, "UTF-8");

		StringWriter sw= new StringWriter();
		objectMapper.writeValue(sw, messageList);

		JSONAssert.assertEquals(sw.toString(), json, true);
	}

}
