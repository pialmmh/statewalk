package com.telcobright.statewalk.v2.session.events;

import com.telcobright.statewalk.v2.registry.consumes.StatemachineEvent;

/** Budget child → supervisor: final totals; drives the terminal transition. */
public record Settled(Object totals) implements StatemachineEvent {}
