package com.example.catalogservice.messagequeue;

import com.example.catalogservice.jpa.CatalogEntity;
import com.example.catalogservice.jpa.CatalogRepository;
import com.example.catalogservice.jpa.OrderId;
import com.example.catalogservice.jpa.OrderIdRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
@Slf4j
public class KafkaConsumer {
    CatalogRepository catalogRepository;
    OrderIdRepository orderConsumeIdRepository;

    @Autowired
    public KafkaConsumer(CatalogRepository catalogRepository, OrderIdRepository orderConsumeIdRepository) {
        this.catalogRepository = catalogRepository;
        this.orderConsumeIdRepository = orderConsumeIdRepository;
    }

    @KafkaListener(topics = "example-catalog-topic")
    public void updateQty(String kafkaMessage) {
        log.info("Kafka Message: ->" + kafkaMessage);

        Map<Object, Object> map = new HashMap<>();
        ObjectMapper mapper = new ObjectMapper();
        try {
            map = mapper.readValue(kafkaMessage, new TypeReference<Map<Object, Object>>() {
            });
        } catch (JsonProcessingException ex) {
            ex.printStackTrace();
        }

        boolean alreadyProceed = orderConsumeIdRepository.existsByOrderId((String) map.get("orderId"));
        log.info("alreadyProceed: " + alreadyProceed);
        if (alreadyProceed) {
            return;
        }
        orderConsumeIdRepository.save(new OrderId((String) map.get("orderId")));

        //이때 락을 걸면 동시성 문제 해결가능?
        CatalogEntity entity = catalogRepository.findByProductId((String) map.get("productId")).orElseThrow(() -> new RuntimeException("Product not found"));
        if (entity.getStock() < 0) throw new IllegalArgumentException("ERROR");
        entity.setStock(entity.getStock() - (Integer) map.get("qty"));
        catalogRepository.save(entity);
    }
}