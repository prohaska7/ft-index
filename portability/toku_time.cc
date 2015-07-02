/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
// vim: ft=cpp:expandtab:ts=8:sw=4:softtabstop=4:
#ident "$Id$"
/*======
This file is part of PerconaFT.


Copyright (c) 2006, 2015, Percona and/or its affiliates. All rights reserved.

    PerconaFT is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License, version 2,
    as published by the Free Software Foundation.

    PerconaFT is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with PerconaFT.  If not, see <http://www.gnu.org/licenses/>.

----------------------------------------

    PerconaFT is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License, version 3,
    as published by the Free Software Foundation.

    PerconaFT is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with PerconaFT.  If not, see <http://www.gnu.org/licenses/>.
======= */

#ident "Copyright (c) 2006, 2015, Percona and/or its affiliates. All rights reserved."

#include "toku_assert.h"
#include "toku_time.h"

#if !defined(HAVE_CLOCK_REALTIME)

#include <errno.h>
#include <mach/clock.h>
#include <mach/mach.h>

int toku_clock_gettime(clockid_t clk_id, struct timespec *ts) {
    if (clk_id != CLOCK_REALTIME) {
        // dunno how to fake any of the other types of clock on osx
        return EINVAL;
    }
    // We may want to share access to cclock for performance, but that requires
    // initialization and destruction that's more complex than it's worth for
    // OSX right now.  Some day we'll probably just use pthread_once or
    // library constructors.
    clock_serv_t cclock;
    mach_timespec_t mts;
    host_get_clock_service(mach_host_self(), REALTIME_CLOCK, &cclock);
    clock_get_time(cclock, &mts);
    mach_port_deallocate(mach_task_self(), cclock);
    ts->tv_sec = mts.tv_sec;
    ts->tv_nsec = mts.tv_nsec;
    return 0;
}

#else // defined(HAVE_CLOCK_REALTIME)

#include <time.h>

int toku_clock_gettime(clockid_t clk_id, struct timespec *ts) {
    return clock_gettime(clk_id, ts);
}

#endif

tokutime_t toku_time_now(void) {
    struct timespec t;
    int r = toku_clock_gettime(CLOCK_REALTIME, &t);
    assert_zero(r);
    return t.tv_sec * 1000000000ULL + t.tv_nsec;
}

double tokutime_to_seconds(tokutime_t t) {
    return (double) t / 1000000000.0;
}

uint64_t toku_current_time_microsec(void) {
    return toku_time_now() / 1000ULL;
}

