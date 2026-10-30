package com.example.lottery.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.LocalDate;

@Component
public class LotteryDrawKafkaListener {
    private final ObjectMapper objectMapper;
    private final LotteryDrawWorker worker;

    public LotteryDrawKafkaListener(ObjectMapper objectMapper, LotteryDrawWorker worker) {
        this.objectMapper = objectMapper;
        this.worker = worker;
    }

    @KafkaListener(topics = "${lottery.kafka.topic}")
    public void onMessage(String payload) throws Exception {
        JsonNode node = objectMapper.readTree(payload);
        if (!node.hasNonNull("drawId")) throw new IllegalArgumentException("Missing drawId in Kafka event");
        long drawId = node.get("drawId").asLong();
        if (node.hasNonNull("campaignMonth")) {
            worker.process(drawId, LocalDate.parse(node.get("campaignMonth").asText()));
        } else {
            // Backward-compatible handling for events published before campaignMonth was added.
            worker.process(drawId);
        }
    }
}
