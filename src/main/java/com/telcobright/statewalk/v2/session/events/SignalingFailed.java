package com.telcobright.statewalk.v2.session.events;

import com.telcobright.statewalk.v2.registry.consumes.StatemachineEvent;

/** Signaling child → supervisor: signaling failed with a domain cause. */
public record SignalingFailed(String cause) implements StatemachineEvent {}
