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

#include <stdio.h>
#include <stdlib.h>
#include <toku_stdint.h>
#include <unistd.h>
#include <toku_assert.h>
#include "toku_os.h"

#if HAVE_SCHED_H
#include <sched.h>

static void check_number_cpus(int ncpus, int cpus_online) {
    // set random cpu's in the cpu set
    cpu_set_t cs;
    if (ncpus == cpus_online) {
        CPU_ZERO(&cs);
        for (int cpu = 0; cpu != ncpus; cpu++) {
            CPU_SET(cpu, &cs);
        }
    } else {
        CPU_ZERO(&cs);
        for (int n = 0; n != ncpus; ) {
            int cpu = rand() % cpus_online;
            if (!CPU_ISSET(cpu, &cs)) {
                CPU_SET(cpu, &cs);
                n++;
            }
        }
    }
    int r = sched_setaffinity(0, sizeof cs, &cs);
    assert(r == 0);
    assert(toku_os_get_number_cpus() == ncpus);
}
#endif

int main(void) {
    // test the default case of unset TOKU_NCPUS and no affinity
    {
        int r = unsetenv("TOKU_NCPUS"); 
        assert(r == 0);
        int cpus_online = sysconf(_SC_NPROCESSORS_ONLN);
        assert(cpus_online > 0);
        assert(toku_os_get_number_cpus() == cpus_online);
    }

    // verify that TOKU_NCPUS env var gets priority
    {
        int r = unsetenv("TOKU_NCPUS"); 
        assert(r == 0);
        int cpus_online = sysconf(_SC_NPROCESSORS_ONLN);
        assert(cpus_online > 0);
        for (int ncpus = 1; ncpus <= cpus_online; ncpus++) {
            char ncpus_str[32];
            sprintf(ncpus_str, "%d", ncpus);
            r = setenv("TOKU_NCPUS", ncpus_str, 1);
            assert(r == 0);
            assert(toku_os_get_number_cpus() == ncpus);
        }
    }

#if HAVE_SCHED_GETAFFINITY
    // test various cpu affinities
    {
        int r = unsetenv("TOKU_NCPUS"); 
        assert(r == 0);
        int cpus_online = sysconf(_SC_NPROCESSORS_ONLN);
        assert(cpus_online >= 0);
        for (int ncpus = 1; ncpus <= cpus_online; ncpus++) {
            for (int nexp = 0; nexp < 100; nexp++) {
                check_number_cpus(ncpus, cpus_online);
            }
        }
    }
#endif

    return 0;
}
