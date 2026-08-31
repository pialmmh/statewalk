package com.telcobright.statewalk.session.events;

import com.telcobright.statewalk.event.StatemachineEvent;

/** Budget child → supervisor: final totals; drives the terminal transition. */
public record Settled(Object totals) implements StatemachineEvent {}
