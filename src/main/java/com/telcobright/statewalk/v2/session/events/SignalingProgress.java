package com.telcobright.statewalk.v2.session.events;

import com.telcobright.statewalk.v2.registry.consumes.StatemachineEvent;

/** Signaling child → supervisor: informative phase report (ringing, portal-opened, …). */
public record SignalingProgress(String phase) implements StatemachineEvent {}
