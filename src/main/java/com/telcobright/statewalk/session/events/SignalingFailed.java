package com.telcobright.statewalk.session.events;

import com.telcobright.statewalk.event.StatemachineEvent;

/** Signaling child → supervisor: signaling failed with a domain cause. */
public record SignalingFailed(String cause) implements StatemachineEvent {}
