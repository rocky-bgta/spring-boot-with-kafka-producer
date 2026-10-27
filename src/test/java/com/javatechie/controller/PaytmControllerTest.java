package com.javatechie.controller;

import com.javatechie.dto.PaymentRequest;
import com.javatechie.dto.PaytmRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class PaytmControllerTest {

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    @InjectMocks
    private PaytmController controller;

    @Test
    void paymentEventsHaveDistinctPayloadsAndDoNotMutateIncomingRequest() {
        ReflectionTestUtils.setField(controller, "topicName", "payment-topic");
        ReflectionTestUtils.setField(controller, "paymentBatchSize", 3);
        PaymentRequest original = new PaymentRequest("original", "source", "destination", 25, null);

        String response = controller.doPayment(new PaytmRequest<>(original));

        ArgumentCaptor<Object> events = ArgumentCaptor.forClass(Object.class);
        verify(kafkaTemplate, times(3)).send(eq("payment-topic"), events.capture());
        List<PaymentRequest> published = events.getAllValues().stream()
                .map(PaymentRequest.class::cast)
                .collect(Collectors.toList());

        assertEquals(3, published.size());
        assertNotSame(published.get(0), published.get(1));
        assertNotSame(published.get(1), published.get(2));
        assertEquals("SRC_AC1", published.get(0).getSrcAc());
        assertEquals("SRC_AC2", published.get(1).getSrcAc());
        assertEquals("SRC_AC3", published.get(2).getSrcAc());
        assertEquals(3, published.stream().map(PaymentRequest::getTransactionId).distinct().count());
        assertEquals("original", original.getTransactionId());
        assertEquals("source", original.getSrcAc());
        assertEquals(25, original.getAmount());
        assertTrue(response.contains("asynchronous"));
    }
}
