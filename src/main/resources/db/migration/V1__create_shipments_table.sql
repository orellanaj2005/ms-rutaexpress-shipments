CREATE SEQUENCE shipments_seq START WITH 1 INCREMENT BY 1;

CREATE TABLE shipments (
    id NUMBER(19) DEFAULT shipments_seq.NEXTVAL PRIMARY KEY,
    origin_address VARCHAR2(255) NOT NULL,
    destination_address VARCHAR2(255) NOT NULL,
    recipient_name VARCHAR2(150) NOT NULL,
    recipient_email VARCHAR2(150) NOT NULL,
    recipient_phone VARCHAR2(30),
    service_id NUMBER(19) NOT NULL,
    weight_kg NUMBER(10,2) NOT NULL,
    declared_value NUMBER(12,2),
    status VARCHAR2(20) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    version NUMBER(10) DEFAULT 0 NOT NULL
);

CREATE INDEX idx_shipments_status ON shipments(status);
CREATE INDEX idx_shipments_created_at ON shipments(created_at);
