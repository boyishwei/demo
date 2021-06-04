package com.kafka.demo;

import com.kafka.demo.handler.JSONFileHandler;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class Application {
	@Bean
	public JSONFileHandler JsonFileHandler(){
		return new JSONFileHandler();
	}

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

}
