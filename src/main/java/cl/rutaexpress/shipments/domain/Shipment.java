package cl.rutaexpress.shipments.domain;

import jakarta.persistence.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.math.BigDecimal;
import java.time.Instant;

@Entity
@Table(name = "shipments")
public class Shipment {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "shipments_seq_gen")
    @SequenceGenerator(name = "shipments_seq_gen", sequenceName = "shipments_seq", allocationSize = 1)
    private Long id;

    @Column(name = "origin_address", nullable = false)
    private String originAddress;

    @Column(name = "destination_address", nullable = false)
    private String destinationAddress;

    @Column(name = "recipient_name", nullable = false)
    private String recipientName;

    @Column(name = "recipient_email", nullable = false)
    private String recipientEmail;

    @Column(name = "recipient_phone")
    private String recipientPhone;

    @Column(name = "service_id", nullable = false)
    private Long serviceId;

    @Column(name = "weight_kg", nullable = false)
    private BigDecimal weightKg;

    @Column(name = "declared_value")
    private BigDecimal declaredValue;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 20)
    private ShipmentStatus status = ShipmentStatus.CREADO;

    // ojdbc11 throws ORA-18716 when Hibernate 7's default JDBC type for
    // Instant (TimestampUtcAsOffsetDateTimeJdbcType) calls
    // getObject(col, OffsetDateTime.class) against a plain TIMESTAMP column.
    // Forcing the classic TIMESTAMP JDBC type keeps Instant as the Java type
    // but reads/writes via getTimestamp()/setTimestamp() instead, avoiding
    // the buggy driver code path.
    @JdbcTypeCode(SqlTypes.TIMESTAMP)
    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @JdbcTypeCode(SqlTypes.TIMESTAMP)
    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @Version
    @Column(name = "version", nullable = false)
    private int version;

    protected Shipment() {
        // JPA
    }

    public Shipment(String originAddress, String destinationAddress, String recipientName,
                     String recipientEmail, String recipientPhone, Long serviceId,
                     BigDecimal weightKg, BigDecimal declaredValue) {
        this.originAddress = originAddress;
        this.destinationAddress = destinationAddress;
        this.recipientName = recipientName;
        this.recipientEmail = recipientEmail;
        this.recipientPhone = recipientPhone;
        this.serviceId = serviceId;
        this.weightKg = weightKg;
        this.declaredValue = declaredValue;
        this.status = ShipmentStatus.CREADO;
        Instant now = Instant.now();
        this.createdAt = now;
        this.updatedAt = now;
    }

    @PrePersist
    protected void onCreate() {
        Instant now = Instant.now();
        if (createdAt == null) {
            createdAt = now;
        }
        updatedAt = now;
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = Instant.now();
    }

    public Long getId() {
        return id;
    }

    public String getOriginAddress() {
        return originAddress;
    }

    public String getDestinationAddress() {
        return destinationAddress;
    }

    public String getRecipientName() {
        return recipientName;
    }

    public String getRecipientEmail() {
        return recipientEmail;
    }

    public String getRecipientPhone() {
        return recipientPhone;
    }

    public Long getServiceId() {
        return serviceId;
    }

    public BigDecimal getWeightKg() {
        return weightKg;
    }

    public BigDecimal getDeclaredValue() {
        return declaredValue;
    }

    public ShipmentStatus getStatus() {
        return status;
    }

    public void setStatus(ShipmentStatus status) {
        this.status = status;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public int getVersion() {
        return version;
    }
}
