package com.telcobright.statewalk.session.events;

import com.telcobright.statewalk.event.StatemachineEvent;

/** Base-internal: published by the supervisor after {@code runAdmission}. */
public record AdmissionDecided(boolean accepted, String cause) implements StatemachineEvent {}
