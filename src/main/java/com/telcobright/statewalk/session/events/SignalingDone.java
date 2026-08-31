package com.telcobright.statewalk.session.events;

import com.telcobright.statewalk.event.StatemachineEvent;

/** Signaling child → supervisor: signaling succeeded; {@code grant} = the domain grant payload. */
public record SignalingDone(Object grant) implements StatemachineEvent {}
