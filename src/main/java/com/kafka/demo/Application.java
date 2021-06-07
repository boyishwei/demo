package com.kafka.demo;

import cn.hutool.core.thread.ThreadUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.demo.constant.AppConstants;
import com.kafka.demo.handler.JSONFileHandler;
import com.kafka.demo.service.KafKaConsumerService;
import com.kafka.demo.service.KafKaProducerService;
import com.kafka.demo.utils.MessageHelper;
import com.kafka.demo.vo.TradeMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.utils.ThreadUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.util.List;

@Slf4j
@SpringBootApplication
@EnableScheduling
@EmbeddedKafka(topics = "trade",count = 4,ports = {9092,9093,9094,9095})
public class Application {
	@Autowired
	JSONFileHandler jsonFileHandler;
	@Autowired
	KafKaProducerService kafKaProducerService;

	@Value("{output.file.one.location}")
	File oupput_one;
	@Value("{output.file.two.location}")
	File oupput_two;

	@Autowired
	private KafkaListenerEndpointRegistry registry;

	@KafkaListener(id="consumer1", groupId = AppConstants.GROUP_ID, autoStartup = "false", topicPartitions = {@TopicPartition(topic = AppConstants.TOPIC_NAME,partitions = {"0"})})
	public void consumer_one(String message) throws IOException {
		jsonFileHandler.writeToJsonFile(oupput_one, message);
		log.info("consumer 1 received message =>", message);
	}

	@KafkaListener(id="consumer2", groupId = AppConstants.GROUP_ID, autoStartup = "false", topicPartitions = {@TopicPartition(topic = AppConstants.TOPIC_NAME,partitions = {"1"})})
	public void consumer_two(String message) throws IOException {
		jsonFileHandler.writeToJsonFile(oupput_two, message);
		log.info("consumer 2 received message =>", message);
	}

	@Scheduled(fixedDelay = 50000, initialDelay = 15000)
	public void scheduledConsumer1() {
		registry.getListenerContainer("consumer1").start();
	}

	@Scheduled(fixedDelay = 50000, initialDelay = 15000)
	public void stopConsumer1() {
		registry.getListenerContainer("consumer1").stop();
	}

	@Scheduled(fixedDelay = 10000, initialDelay = 5000)
	public void scheduledConsumer2() {
		registry.getListenerContainer("consumer2").start();
	}

	@Scheduled(fixedDelay = 10000, initialDelay = 5000)
	public void stopConsumer2() {
		registry.getListenerContainer("consumer2").stop();
	}
	@PostConstruct
	public void producer() {
		new Thread(() -> {
			while (true) {
				if (jsonFileHandler.getInputFile().canRead()) {
					List<TradeMessage> messageList = null;
					try {
						messageList = jsonFileHandler.read();
					} catch (IOException e) {
						e.printStackTrace();
					}
					messageList.stream().filter(MessageHelper::validate).map(MessageHelper::enrich).forEach(kafKaProducerService::sendMessage); jsonFileHandler.archive();
				} else {
					ThreadUtil.sleep(10000);
				}
			}
		}).start();
	}

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}
}
