package com.acuver.aekyam.slotpoc;

import com.acuver.aekyam.slotpoc.config.SlotPocProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(SlotPocProperties.class)
public class SlotPocApplication {

    public static void main(String[] args) {
        SpringApplication.run(SlotPocApplication.class, args);
    }
}
