package com.example.lottery.config;

import com.example.lottery.domain.WeightedPrizeSelector;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class LotteryConfig {
    @Bean
    WeightedPrizeSelector weightedPrizeSelector() {
        return new WeightedPrizeSelector();
    }
}
