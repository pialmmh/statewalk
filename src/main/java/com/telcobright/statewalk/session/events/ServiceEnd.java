package com.telcobright.statewalk.session.events;

import com.telcobright.statewalk.event.StatemachineEvent;

/**
 * Any end trigger, wire- or child-originated: budget exhausted, media/idle
 * timeout, external revoke, admin kick, device gone, hangup. While ACTIVE it
 * drives TEARING_DOWN; while ADMITTED it aborts to FAILED.
 */
public record ServiceEnd(String cause) implements StatemachineEvent {}
