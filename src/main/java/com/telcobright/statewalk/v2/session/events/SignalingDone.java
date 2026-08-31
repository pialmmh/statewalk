package com.telcobright.statewalk.v2.session.events;

import com.telcobright.statewalk.v2.registry.consumes.StatemachineEvent;

/** Signaling child → supervisor: signaling succeeded; {@code grant} = the domain grant payload. */
public record SignalingDone(Object grant) implements StatemachineEvent {}
