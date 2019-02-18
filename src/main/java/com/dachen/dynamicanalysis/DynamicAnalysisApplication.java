package com.dachen.dynamicanalysis;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan("com.dachen")
public class DynamicAnalysisApplication {

	public static void main(String[] args) {
		SpringApplication.run(DynamicAnalysisApplication.class, args);
	}
}
