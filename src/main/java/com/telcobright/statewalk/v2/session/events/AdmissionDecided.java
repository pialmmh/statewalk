package com.telcobright.statewalk.v2.session.events;

import com.telcobright.statewalk.v2.registry.consumes.StatemachineEvent;

/** Base-internal: published by the supervisor after {@code runAdmission}. */
public record AdmissionDecided(boolean accepted, String cause) implements StatemachineEvent {}
