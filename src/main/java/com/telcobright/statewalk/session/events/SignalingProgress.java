package com.telcobright.statewalk.session.events;

import com.telcobright.statewalk.event.StatemachineEvent;

/** Signaling child → supervisor: informative phase report (ringing, portal-opened, …). */
public record SignalingProgress(String phase) implements StatemachineEvent {}
