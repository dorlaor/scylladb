ScyllaDB File Descriptor Guard (scylla-fd-guard)
================================================

ScyllaDB keeps a very large number of files and sockets open. If it reaches
its open-file limit (``RLIMIT_NOFILE``), the kernel fails further ``open()``
calls with *Too many open files* (``EMFILE``) and the node degrades or stops.

``scylla-fd-guard`` is a small watchdog daemon that:

* polls how many file descriptors the ScyllaDB process has open,
* logs a warning to the journal when usage crosses a threshold of the limit
  (80% by default),
* **raises the limit at runtime** — with neither a reboot nor a ScyllaDB
  restart — when usage crosses a higher threshold (90% by default), using
  the ``prlimit(2)`` system call,
* persists the raised limit in a systemd drop-in so it survives the next
  ScyllaDB restart,
* optionally runs a notification hook and exports Prometheus metrics.

Background
----------

ScyllaDB already raises its *soft* limit to the *hard* limit at startup and
refuses to start when the hard limit is too low. ``scylla_setup`` sizes
``LimitNOFILE`` for the machine (at least 800000). What no component does on
its own is react when a running node *approaches* the limit: a process cannot
raise its own hard limit, since that requires ``CAP_SYS_RESOURCE``, which
ScyllaDB (running as the unprivileged ``scylla`` user) does not have.
``scylla-fd-guard`` runs privileged and does it from the outside. The kernel
ceiling for the raise is ``fs.nr_open``, which ScyllaDB packaging already
sets to about one billion, so there is ample headroom without touching any
sysctl.

Enabling
--------

The service is installed with ScyllaDB but not enabled by default:

.. code-block:: bash

   sudo systemctl enable --now scylla-fd-guard

Watch its decisions with:

.. code-block:: bash

   journalctl -u scylla-fd-guard -f

All messages are logged with proper syslog priorities, so
``journalctl -p warning -u scylla-fd-guard`` shows only the events worth
alerting on (thresholds crossed, limits raised, cap reached).

How it decides
--------------

Every ``--interval`` seconds (default 10) the guard counts the entries in
``/proc/<pid>/fd`` of the ScyllaDB main process and compares them with its
soft limit — the limit ``open()`` actually enforces:

* below ``--warn-ratio`` (default 0.8): nothing to do.
* above ``--warn-ratio``: log one warning (re-armed after usage drops).
* above ``--raise-ratio`` (default 0.9): raise both limits to
  ``current soft × --raise-factor`` (default 2.0), never above
  ``--max-nofile`` (default: ``fs.nr_open``). Raises are rate-limited by
  ``--raise-cooldown`` (default 60 s). After a raise the new value is written
  to ``/etc/systemd/system/scylla-server.service.d/99-fd-guard.conf``
  followed by ``systemctl daemon-reload``, so a later ScyllaDB restart keeps
  it.
* if the limit is already at the cap, an error is logged instead.

The guard also watches the system-wide count (``fs.file-nr`` against
``fs.file-max``) and logs an error when the whole machine runs low.

A raise is a symptom, not a fix
-------------------------------

The limit exists partly as a leak detector. The guard keeps the node alive,
but every raise is logged at warning priority and reported through the
notification hook precisely because it usually means either a file descriptor
leak or an under-provisioned initial limit — investigate rather than rely on
repeated raises. Use ``--no-raise`` to run the guard as a pure monitor, or
``--max-nofile`` to bound how far it may go.

Notifications and metrics
-------------------------

``--notify-command CMD`` runs a shell command on every event, with details
in environment variables: ``SCYLLA_FD_GUARD_EVENT`` (``warn``, ``raise``,
``cap`` or ``system_warn``), ``SCYLLA_FD_GUARD_PID``, ``SCYLLA_FD_GUARD_FDS``,
``SCYLLA_FD_GUARD_SOFT``, ``SCYLLA_FD_GUARD_HARD``,
``SCYLLA_FD_GUARD_NEW_SOFT`` and ``SCYLLA_FD_GUARD_NEW_HARD``. Point it at a
webhook, a pager script, or ``wall``.

``--metrics-file PATH`` writes Prometheus textfile-collector format on every
check. Point it into node_exporter's ``--collector.textfile.directory`` (part
of the ScyllaDB monitoring stack) to get, per node:

* ``scylla_fd_guard_open_fds``, ``scylla_fd_guard_limit_soft``,
  ``scylla_fd_guard_limit_hard`` — alert when
  ``open_fds / limit_soft > 0.8``,
* ``scylla_fd_guard_raises_total`` — alert on any increase,
* ``scylla_fd_guard_sys_open_files`` / ``scylla_fd_guard_sys_max_files``.

To pass options, override the unit:

.. code-block:: bash

   sudo systemctl edit scylla-fd-guard

.. code-block:: ini

   [Service]
   ExecStart=
   ExecStart=/opt/scylladb/scripts/scylla_fd_guard --unit scylla-server.service \
       --metrics-file /var/lib/node_exporter/fd_guard.prom \
       --notify-command '/usr/local/bin/page-oncall fd-guard'

Options reference
-----------------

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Option
     - Meaning
   * - ``--unit UNIT``
     - systemd unit whose MainPID to watch (default ``scylla-server.service``)
   * - ``--pid PID``
     - watch a raw PID instead of a unit (mainly for testing)
   * - ``--interval SECONDS``
     - seconds between checks (default 10)
   * - ``--warn-ratio R``
     - warn above this fraction of the soft limit (default 0.8)
   * - ``--raise-ratio R``
     - raise the limit above this fraction (default 0.9)
   * - ``--raise-factor F``
     - growth factor per raise (default 2.0)
   * - ``--max-nofile N``
     - never raise beyond N (default: ``fs.nr_open``)
   * - ``--raise-cooldown SECONDS``
     - minimum time between raises (default 60)
   * - ``--no-raise``
     - monitor and notify only, never change limits
   * - ``--no-persist`` / ``--persist-path PATH``
     - disable or redirect the systemd drop-in written after a raise
   * - ``--notify-command CMD``
     - shell hook run on warn/raise/cap events
   * - ``--metrics-file PATH``
     - Prometheus textfile metrics output
   * - ``--dry-run``
     - log intended actions without performing them
   * - ``--oneshot``
     - check once and exit (for cron or testing)

Privileges
----------

Raising a hard limit requires ``CAP_SYS_RESOURCE`` and reading
``/proc/<pid>/fd`` of another user's process requires ``CAP_SYS_PTRACE``;
the shipped unit runs as root with the capability bounding set reduced to
just what is needed. Without privileges (for example in a non-root install)
the guard still monitors, notifies, and raises the soft limit up to the
existing hard limit.

Manual alternative
------------------

The same runtime raise can always be done by hand, without restarting
ScyllaDB:

.. code-block:: bash

   sudo prlimit --pid $(systemctl show -p MainPID --value scylla-server) --nofile=1600000:1600000

Testing
-------

The test suite lives in ``test/dist_test/test_fd_guard.py``. Run it directly
with pytest, or in a container via
``test/dist_test/test_fd_guard_container.sh``; give the container real root
(``--privileged`` on rootful Docker, or ``--runtime /usr/bin/krun`` with
libkrun for a rootless micro-VM) to also exercise the hard-limit raises.
