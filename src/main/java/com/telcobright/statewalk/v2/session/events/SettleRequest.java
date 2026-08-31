package com.telcobright.statewalk.v2.session.events;

import com.telcobright.statewalk.v2.registry.consumes.StatemachineEvent;

/** Supervisor → budget child (async-settle domains): produce the final totals. */
public record SettleRequest() implements StatemachineEvent {}
