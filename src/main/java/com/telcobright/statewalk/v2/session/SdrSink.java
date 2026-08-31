package com.telcobright.statewalk.v2.session;

/**
 * Where finished sessions go (file, stream, Kafka — host's choice). Must not
 * block the cell thread for long and must not throw; the base swallows a
 * throw so a sink failure can never crash a machine, but the record is then
 * lost — implementations should buffer internally.
 */
@FunctionalInterface
public interface SdrSink {

    void write(SdrRecord record);
}
