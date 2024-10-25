package com.example.catalogservice.jpa;

import org.springframework.data.repository.CrudRepository;

public interface OrderIdRepository extends CrudRepository<OrderId, Long> {

    boolean existsByOrderId(String orderId);
}
