package com.telcobright.paymentgw;

/**
 * Where finished payments go (DB table, Kafka, file — host's choice). Must not
 * block long and should not throw; a throw is logged with the payment id and
 * the record is lost from the sink's point of view — implementations should
 * buffer internally.
 */
@FunctionalInterface
public interface PaymentRecordSink {

    void write(PaymentRecord record);
}
