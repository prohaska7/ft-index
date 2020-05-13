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

#include "lock_request_unit_test.h"

namespace toku {

// Verify that the deadlock detector works on a graph with the following
// wait for edges:
// 1 waits for 2, 2 waits for 1
void lock_request_unit_test::run(void) {
    lock_request_info mgr;
    mgr.init();

    const int N = 2;
    lock_request lock_request[N];
    for (int i=0; i<N; i++)
        lock_request[i].create();

    // Add 1 -> 2
    lock_request[0].m_txnid = 1;
    lock_request[0].m_conflicts.add(2);
    mgr.add_to_pending(&lock_request[0]);
    assert(!mgr.deadlock_exists(&lock_request[0]));

    // Add 2 -> 1
    lock_request[1].m_txnid = 2;
    lock_request[1].m_conflicts.add(1);
    mgr.add_to_pending(&lock_request[1]);
    assert(mgr.deadlock_exists(&lock_request[0]));
    assert(mgr.deadlock_exists(&lock_request[1]));

    for (int i=0; i<N; i++)
        mgr.remove_from_pending(&lock_request[i]);

    for (int i=0; i<N; i++)
        lock_request[i].destroy();

    mgr.destroy();
}

} /* namespace toku */

int main(void) {
    toku::lock_request_unit_test test;
    test.run();
    return 0;
}

