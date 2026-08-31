package com.telcobright.statewalk.session.events;

import com.telcobright.statewalk.event.StatemachineEvent;

/** Supervisor → budget child (async-settle domains): produce the final totals. */
public record SettleRequest() implements StatemachineEvent {}
