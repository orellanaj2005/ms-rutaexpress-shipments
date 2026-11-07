package cl.rutaexpress.shipments.messaging;

import cl.rutaexpress.shipments.domain.Shipment;
import cl.rutaexpress.shipments.domain.ShipmentStatus;
import cl.rutaexpress.shipments.dto.CreateShipmentRequest;
import cl.rutaexpress.shipments.exception.InvalidStatusTransitionException;
import cl.rutaexpress.shipments.service.ShipmentService;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Flujo completo contra un broker Kafka embebido: crear y cancelar un envío deja en
 * shipments.events y audit.timeline los mensajes del contrato (docs/eventos.md), con el shipmentId
 * como clave. Un cambio de estado rechazado no publica nada.
 */
@SpringBootTest(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 3, topics = {"shipments.events", "audit.timeline"})
class ShipmentKafkaFlowIntegrationTest {

    @MockitoBean
    JwtDecoder jwtDecoder;

    @Autowired
    ShipmentService shipmentService;

    @Autowired
    EmbeddedKafkaBroker broker;

    @Autowired
    ObjectMapper objectMapper;

    private Consumer<String, String> consumer;

    @BeforeEach
    void setUp() {
        Map<String, Object> props = KafkaTestUtils.consumerProps(broker, "test-" + System.nanoTime(), false);
        props.put("auto.offset.reset", "latest");
        consumer = new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), new StringDeserializer())
                .createConsumer();
        broker.consumeFromEmbeddedTopics(consumer, "shipments.events", "audit.timeline");
        // Con "latest", el consumidor debe tener particiones asignadas antes de que se publique.
        KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(2));

        Jwt jwt = Jwt.withTokenValue("token").header("alg", "none")
                .claim("preferred_username", "operador@rutaexpress.cl")
                .claim("roles", List.of("Operador"))
                .build();
        SecurityContextHolder.getContext().setAuthentication(
                new JwtAuthenticationToken(jwt, List.of(new SimpleGrantedAuthority("Operador"))));
    }

    @AfterEach
    void tearDown() {
        SecurityContextHolder.clearContext();
        consumer.close();
    }

    @Test
    void createAndCancelPublishContractEvents() {
        Shipment shipment = shipmentService.create(new CreateShipmentRequest("Origen 1", "Destino 2", "Ana",
                "ana@correo.cl", null, 3L, new BigDecimal("1.5"), null));
        shipmentService.changeStatus(shipment.getId(), ShipmentStatus.CANCELADO);
        String key = shipment.getId().toString();

        List<ConsumerRecord<String, String>> records = poll(4);

        assertThat(records).allSatisfy(record -> assertThat(record.key()).isEqualTo(key));
        assertThat(records).allSatisfy(record -> assertThat(record.headers().lastHeader("__TypeId__")).isNull());

        List<JsonNode> events = byTopic(records, "shipments.events");
        assertThat(events).extracting(e -> e.get("type").asText())
                .containsExactly("ShipmentCreated", "ShipmentStatusChanged");
        JsonNode created = events.get(0);
        assertThat(created.get("version").asInt()).isEqualTo(1);
        assertThat(created.get("aggregateId").asText()).isEqualTo(key);
        assertThat(created.get("producer").asText()).isEqualTo("ms-rutaexpress-shipments");
        assertThat(created.get("timestamp").isString()).as("timestamp en ISO-8601").isTrue();
        assertThat(created.at("/payload/status").asText()).isEqualTo("CREADO");
        assertThat(created.at("/payload/createdBy").asText()).isEqualTo("operador@rutaexpress.cl");
        JsonNode changed = events.get(1);
        assertThat(changed.at("/payload/status").asText()).isEqualTo("CANCELADO");
        assertThat(changed.at("/payload/previousStatus").asText()).isEqualTo("CREADO");
        assertThat(changed.path("payload").has("newStatus")).isFalse();

        List<JsonNode> audit = byTopic(records, "audit.timeline");
        assertThat(audit).extracting(e -> e.at("/payload/action").asText())
                .containsExactly("CREAR_ENVIO", "CANCELAR_ENVIO");
        assertThat(audit).allSatisfy(e -> assertThat(e.at("/payload/role").asText()).isEqualTo("Operador"));
        assertThat(audit.get(1).get("traceId").asText()).isEqualTo(changed.get("traceId").asText());
    }

    @Test
    void rejectedTransitionPublishesNothing() {
        Shipment shipment = shipmentService.create(new CreateShipmentRequest("Origen 1", "Destino 2", "Ana",
                "ana@correo.cl", null, 3L, new BigDecimal("1.5"), null));
        poll(2);

        assertThatThrownBy(() -> shipmentService.changeStatus(shipment.getId(), ShipmentStatus.ENTREGADO))
                .isInstanceOf(InvalidStatusTransitionException.class);

        assertThat(KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(3)).count()).isZero();
    }

    private List<ConsumerRecord<String, String>> poll(int expected) {
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        long deadline = System.currentTimeMillis() + 15_000;
        while (records.size() < expected && System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, String> batch = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(1), 1);
            batch.forEach(records::add);
        }
        assertThat(records).hasSize(expected);
        return records;
    }

    private List<JsonNode> byTopic(List<ConsumerRecord<String, String>> records, String topic) {
        return records.stream()
                .filter(r -> r.topic().equals(topic))
                .sorted((a, b) -> Long.compare(a.offset(), b.offset()))
                .map(r -> objectMapper.readTree(r.value()))
                .toList();
    }
}
