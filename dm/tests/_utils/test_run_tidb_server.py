#!/usr/bin/env python3
# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Exercise startup polling without a database or real retry sleeps.

Run with: python3 dm/tests/_utils/test_run_tidb_server.py
"""

import os
from pathlib import Path
import signal
import subprocess
import tempfile
import unittest


HELPER = Path(__file__).with_name("run_tidb_server").resolve()


class TiDBStartupTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.test_dir = self.root / "data"
        self.log = self.test_dir / "downstream/tidb/log/tidb.log"
        self.log.parent.mkdir(parents=True)
        self.env = dict(os.environ, PATH=f"{self.bin}:{os.environ['PATH']}",
                        TEST_DIR=str(self.test_dir), NEXT_GEN="0", PD_ADDR="",
                        STARTUP_TEST_DIR=str(self.root), READY_AFTER="0")
        self.script("sleep", "#!/bin/sh\nexit 0\n")
        self.script("mysql", """#!/usr/bin/env python3
import os
from pathlib import Path
import sys
import time
root = Path(os.environ['STARTUP_TEST_DIR'])
if 'select * from mysql.tidb;' not in sys.argv:
    sys.exit(0)
for _ in range(200):
    if (root / 'server-started').exists():
        break
    time.sleep(0.01)
p = root / 'attempts'
attempt = int(p.read_text()) + 1 if p.exists() else 1
p.write_text(str(attempt))
sys.exit(0 if attempt > int(os.environ['READY_AFTER']) else 1)
""")
        self.script("tidb-server", """#!/usr/bin/env python3
import os
from pathlib import Path
import time
root = Path(os.environ['STARTUP_TEST_DIR'])
(root / 'server-started').write_text(str(os.getpid()))
time.sleep(30)
""")
        self.addCleanup(self.stop_server)

    def script(self, name, content):
        path = self.bin / name
        path.write_text(content)
        path.chmod(0o755)

    def stop_server(self):
        pid_file = self.root / "server-started"
        if pid_file.exists():
            try:
                os.kill(int(pid_file.read_text()), signal.SIGTERM)
            except ProcessLookupError:
                pass

    def run_helper(self):
        # Redirect to a file so a live background server cannot hold a PIPE open.
        with (self.root / "output").open("w") as output:
            result = subprocess.run(["sh", str(HELPER), "4000", "test"],
                                    cwd=self.root, env=self.env, stdout=output,
                                    stderr=subprocess.STDOUT, timeout=15)
        return result.returncode, (self.root / "output").read_text()

    def attempts(self):
        return int((self.root / "attempts").read_text())

    def test_ready_immediately(self):
        code, _ = self.run_helper()
        self.assertEqual(code, 0)
        self.assertEqual(self.attempts(), 1)

    def test_nextgen_allows_slow_bootstrap(self):
        self.env.update(NEXT_GEN="1", READY_AFTER="12")
        code, _ = self.run_helper()
        self.assertEqual(code, 0)
        self.assertEqual(self.attempts(), 13)

    def test_classic_preserves_its_retry_budget(self):
        self.env["READY_AFTER"] = "12"
        code, output = self.run_helper()
        self.assertNotEqual(code, 0)
        self.assertEqual(self.attempts(), 11)
        self.assertIn("20 seconds of polling", output)

    def test_nextgen_timeout_includes_log_tail(self):
        self.env.update(NEXT_GEN="1", READY_AFTER="100")
        self.log.write_text("bootstrap is still running\n")
        code, output = self.run_helper()
        self.assertNotEqual(code, 0)
        self.assertEqual(self.attempts(), 61)
        self.assertIn("120 seconds of polling", output)
        self.assertIn("bootstrap is still running", output)

    def test_exited_server_fails_without_exhausting_retries(self):
        self.env.update(NEXT_GEN="1", READY_AFTER="100")
        self.script("tidb-server", """#!/bin/sh
echo 'bootstrap failed' > "$TEST_DIR/downstream/tidb/log/tidb.log"
echo $$ > "$STARTUP_TEST_DIR/server-started"
exit 42
""")
        code, output = self.run_helper()
        self.assertNotEqual(code, 0)
        self.assertLess(self.attempts(), 3)
        self.assertIn("exit status: 42", output)
        self.assertIn("bootstrap failed", output)


if __name__ == "__main__":
    unittest.main()
