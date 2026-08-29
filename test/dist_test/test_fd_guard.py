#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""Tests for dist/common/scripts/scylla_fd_guard.

The unit tests run anywhere. The end-to-end tests spawn a victim process,
run the guard against it in --oneshot mode and check the observable effects
(rlimit changes, journald-style log lines, notify hook, metrics file,
systemd drop-in). Tests that raise the *hard* limit need CAP_SYS_RESOURCE
and are skipped elsewhere; run them as root, e.g. in a privileged container
or a krun/qemu VM via test_fd_guard_container.sh.
"""
import importlib.util
import importlib.machinery
import os
import resource
import subprocess
import sys

import pytest


_GUARD_PATH = os.path.abspath(os.path.join(
    os.path.dirname(__file__), '..', '..', 'dist', 'common', 'scripts',
    'scylla_fd_guard',
))


def _load_guard_module():
    loader = importlib.machinery.SourceFileLoader('scylla_fd_guard', _GUARD_PATH)
    spec = importlib.util.spec_from_loader('scylla_fd_guard', loader)
    mod = importlib.util.module_from_spec(spec)
    loader.exec_module(mod)
    return mod


fd_guard = _load_guard_module()

INF = resource.RLIM_INFINITY
NR_OPEN = 1073741816


def _have_cap_sys_resource():
    """CAP_SYS_RESOURCE (bit 24) in this process's effective capability set."""
    with open('/proc/self/status') as f:
        for line in f:
            if line.startswith('CapEff:'):
                return bool(int(line.split()[1], 16) & (1 << 24))
    return False


requires_cap_sys_resource = pytest.mark.skipif(
    not _have_cap_sys_resource(),
    reason='raising a hard limit requires CAP_SYS_RESOURCE '
           '(run in a privileged container or VM)')


class TestPlanAction:
    """The pure decision function."""

    def plan(self, fds, soft, hard, nr_open=NR_OPEN, warn=0.8, raise_=0.9,
             factor=2.0, cap=NR_OPEN):
        return fd_guard.plan_action(fds, soft, hard, nr_open, warn, raise_,
                                    factor, cap)

    def test_ok_below_warn(self):
        assert self.plan(100, 200, 4096).event == 'ok'

    def test_warn_between_thresholds(self):
        assert self.plan(170, 200, 4096).event == 'warn'

    def test_raise_doubles_soft_limit(self):
        d = self.plan(190, 200, 4096)
        assert d == ('raise', 400, 4096)

    def test_raise_grows_hard_limit_when_needed(self):
        d = self.plan(240, 256, 256)
        assert d == ('raise', 512, 512)

    def test_spike_beyond_factor_still_clears_usage(self):
        # usage jumped far past soft * factor between two polls
        d = self.plan(900, 200, 4096)
        assert d.event == 'raise'
        assert d.new_soft == 901
        assert d.new_hard == 4096

    def test_cap_clamps_the_raise(self):
        d = self.plan(240, 256, 256, cap=300)
        assert d == ('raise', 300, 300)

    def test_at_cap_cannot_raise(self):
        d = self.plan(240, 256, 256, cap=256)
        assert d.event == 'cap'

    def test_infinity_soft_measured_against_nr_open(self):
        assert self.plan(850, INF, INF, nr_open=1000).event == 'warn'
        assert self.plan(100, INF, INF, nr_open=1000).event == 'ok'

    def test_nr_open_bounds_the_cap(self):
        d = self.plan(950, 1000, 1000, nr_open=1024)
        assert d == ('raise', 1024, 1024)


class TestMetrics:
    def test_prometheus_textfile_format(self, tmp_path):
        path = str(tmp_path / 'fd_guard.prom')
        fd_guard.write_metrics(path, {
            'scylla_fd_guard_open_fds': ('gauge', 'open fds', 123),
        })
        text = open(path).read()
        assert '# TYPE scylla_fd_guard_open_fds gauge' in text
        assert 'scylla_fd_guard_open_fds 123' in text


def spawn_victim(nfds, soft, hard):
    """Spawn a process that opens file descriptors until it holds at least
    nfds of them, then lowers its own RLIMIT_NOFILE to (soft, hard)."""
    code = f'''
import os, resource, sys, time
while len(os.listdir('/proc/self/fd')) < {nfds}:
    os.open('/dev/null', os.O_RDONLY)
resource.setrlimit(resource.RLIMIT_NOFILE, ({soft}, {hard}))
print('ready', flush=True)
time.sleep(120)
'''
    p = subprocess.Popen([sys.executable, '-c', code],
                         stdout=subprocess.PIPE, encoding='utf-8')
    assert p.stdout.readline().strip() == 'ready'
    return p


def run_guard(pid, *extra):
    return subprocess.run(
        [sys.executable, _GUARD_PATH, '--oneshot', '--pid', str(pid), *extra],
        capture_output=True, encoding='utf-8', timeout=60)


@pytest.fixture
def victim_factory():
    victims = []

    def make(nfds, soft, hard):
        p = spawn_victim(nfds, soft, hard)
        victims.append(p)
        return p

    yield make
    for p in victims:
        p.kill()
        p.wait()


class TestEndToEnd:
    def test_below_thresholds_does_nothing(self, victim_factory):
        p = victim_factory(nfds=50, soft=200, hard=4096)
        r = run_guard(p.pid)
        assert r.returncode == 0
        assert '<4>' not in r.stderr and '<3>' not in r.stderr
        assert resource.prlimit(p.pid, resource.RLIMIT_NOFILE) == (200, 4096)

    def test_warn_threshold_logs_but_keeps_limits(self, victim_factory):
        p = victim_factory(nfds=170, soft=200, hard=4096)
        r = run_guard(p.pid)
        assert r.returncode == 0
        assert '<4>' in r.stderr and 'crossed 80%' in r.stderr
        assert resource.prlimit(p.pid, resource.RLIMIT_NOFILE) == (200, 4096)

    def test_soft_limit_raise(self, victim_factory):
        # 190/200 = 95% > 90%: the guard must raise the soft limit; the hard
        # limit already has room, so no privilege is needed.
        p = victim_factory(nfds=190, soft=200, hard=4096)
        r = run_guard(p.pid)
        assert r.returncode == 0
        assert 'raised NOFILE limits' in r.stderr
        assert resource.prlimit(p.pid, resource.RLIMIT_NOFILE) == (400, 4096)

    def test_dry_run_changes_nothing(self, victim_factory):
        p = victim_factory(nfds=190, soft=200, hard=4096)
        r = run_guard(p.pid, '--dry-run')
        assert r.returncode == 0
        assert 'dry-run: would raise' in r.stderr
        assert resource.prlimit(p.pid, resource.RLIMIT_NOFILE) == (200, 4096)

    def test_no_raise_mode_only_reports(self, victim_factory):
        p = victim_factory(nfds=190, soft=200, hard=4096)
        r = run_guard(p.pid, '--no-raise')
        assert r.returncode == 0
        assert '<3>' in r.stderr and 'raising is disabled' in r.stderr
        assert resource.prlimit(p.pid, resource.RLIMIT_NOFILE) == (200, 4096)

    def test_notify_metrics_and_persist_on_raise(self, victim_factory, tmp_path):
        p = victim_factory(nfds=190, soft=200, hard=4096)
        notify_out = tmp_path / 'notify.out'
        metrics = tmp_path / 'fd_guard.prom'
        dropin = tmp_path / 'unit.d' / '99-fd-guard.conf'
        r = run_guard(
            p.pid,
            '--notify-command',
            f'echo "$SCYLLA_FD_GUARD_EVENT $SCYLLA_FD_GUARD_NEW_SOFT '
            f'$SCYLLA_FD_GUARD_NEW_HARD" >> {notify_out}',
            '--metrics-file', str(metrics),
            '--persist-path', str(dropin))
        assert r.returncode == 0
        assert resource.prlimit(p.pid, resource.RLIMIT_NOFILE) == (400, 4096)
        assert notify_out.read_text().strip() == 'raise 400 4096'
        metrics_text = metrics.read_text()
        assert 'scylla_fd_guard_raises_total 1' in metrics_text
        assert 'scylla_fd_guard_limit_soft 400' in metrics_text
        assert 'LimitNOFILE=4096' in dropin.read_text()

    def test_metrics_written_when_healthy(self, victim_factory, tmp_path):
        p = victim_factory(nfds=50, soft=200, hard=4096)
        metrics = tmp_path / 'fd_guard.prom'
        r = run_guard(p.pid, '--metrics-file', str(metrics))
        assert r.returncode == 0
        text = metrics.read_text()
        assert 'scylla_fd_guard_limit_hard 4096' in text
        assert 'scylla_fd_guard_sys_max_files' in text

    @requires_cap_sys_resource
    def test_hard_limit_raise(self, victim_factory):
        # 240/256 = 94% and soft == hard: only CAP_SYS_RESOURCE allows
        # growing the hard limit. This is the production Scylla case, since
        # Scylla raises soft to hard at startup.
        p = victim_factory(nfds=240, soft=256, hard=256)
        r = run_guard(p.pid)
        assert r.returncode == 0
        assert 'raised NOFILE limits' in r.stderr
        assert resource.prlimit(p.pid, resource.RLIMIT_NOFILE) == (512, 512)

    @requires_cap_sys_resource
    def test_hard_limit_raise_respects_cap(self, victim_factory):
        p = victim_factory(nfds=240, soft=256, hard=256)
        r = run_guard(p.pid, '--max-nofile', '300')
        assert r.returncode == 0
        assert resource.prlimit(p.pid, resource.RLIMIT_NOFILE) == (300, 300)

    def test_at_cap_reports_error(self, victim_factory):
        # the limit is already at the cap: nothing to raise, report loudly
        p = victim_factory(nfds=240, soft=256, hard=256)
        r = run_guard(p.pid, '--max-nofile', '256')
        assert r.returncode == 0
        assert '<3>' in r.stderr and 'cannot be raised further' in r.stderr
        assert resource.prlimit(p.pid, resource.RLIMIT_NOFILE) == (256, 256)

    def test_unprivileged_falls_back_to_soft_raise(self, victim_factory):
        if _have_cap_sys_resource():
            pytest.skip('needs an unprivileged environment')
        # soft can still double within the existing hard limit headroom,
        # even though the ideal plan would also grow the hard limit
        p = victim_factory(nfds=190, soft=200, hard=350)
        r = run_guard(p.pid)
        assert r.returncode == 0
        assert 'no CAP_SYS_RESOURCE' in r.stderr
        assert resource.prlimit(p.pid, resource.RLIMIT_NOFILE) == (350, 350)
