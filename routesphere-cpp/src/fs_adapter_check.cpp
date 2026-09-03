// Compile-only check that the in-process FreeSWITCH adapter builds against the
// installed FreeSWITCH headers (no link against libfreeswitch here).
#include "statewalk/fs/freeswitch_channel.hpp"

namespace {
[[maybe_unused]] void instantiate() {
    statewalk::fs::FreeSwitchChannel ch("check", {"CHANNEL_PARK"});
    (void)ch.name();
}
}
