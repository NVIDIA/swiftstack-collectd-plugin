# Copyright (c) 2011-2020, NVIDIA CORPORATION. All rights reserved.

import contextlib
import os
import json
import mock
import shutil
import inspect
import unittest
import tempfile
from six.moves.queue import Queue, Empty

from collectd_plugin import collectd_plugin


class TestRead(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        self.metrics_queue = Queue()
        self.node_uuid = "0e1da3a6-6531-4a1b-951a-ad099d259753"
        self.data = {
            "metrics_queue": self.metrics_queue,
            "node_uuid": self.node_uuid,
        }
        self.tempdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tempdir, ignore_errors=True)

    @mock.patch.multiple(
        "collectd_plugin.collectd_plugin",
        time=mock.DEFAULT,
        network_stats=mock.DEFAULT,
        xfs_stats=mock.DEFAULT,
        recon_stats=mock.DEFAULT,
        replication_progress_stats=mock.DEFAULT,
    )
    def test_read_plumbing(
        self, time, network_stats, xfs_stats, recon_stats, replication_progress_stats
    ):
        stub_time = time.return_value = 1380210264.539025
        self.assertEqual(1380210264.539025, collectd_plugin.time())

        stub_time_int = int(stub_time)

        network_stats.return_value = [
            ("net.stat.foobie", (stub_time, 123.394)),
            ("net.stat.barbie", (stub_time, 492)),
        ]
        xfs_stats.return_value = [
            ("xfs.get.jiggy", (stub_time, 323)),
            ("xfs.with.it", (stub_time, 59343945)),
        ]
        recon_stats.return_value = [
            ("swift.recon.party", (stub_time + 7, 4839)),
            ("swift.recon.time", (stub_time - 1, stub_time)),
        ]
        replication_progress_stats.return_value = [
            ("replication.d1.accounts.primary", (stub_time, 1)),
            ("replication.d1.accounts.handoff", (stub_time, 2)),
            ("replication.d1.containers.primary", (stub_time, 3)),
            ("replication.d1.containers.handoff", (stub_time, 4)),
            ("replication.d1.objects.primary", (stub_time, 5)),
            ("replication.d1.objects.handoff", (stub_time, 6)),
        ]

        collectd_plugin.read(self.data)

        got_metrics = []
        while True:
            try:
                got_metrics.append(self.metrics_queue.get_nowait())
            except Empty:
                break

        # Whatever the collectors return is just used (i.e. the stub returns
        # hi-res timestamps, so we expect hi-res timestamps even if the
        # collectors will really be using the timestamp they're handed and
        # they'll be handed an int() of the hi-res time).
        self.assertListEqual(
            [
                [
                    ("%s.net.stat.foobie" % self.node_uuid, (stub_time, 123.394)),
                    ("%s.net.stat.barbie" % self.node_uuid, (stub_time, 492)),
                    ("%s.xfs.get.jiggy" % self.node_uuid, (stub_time, 323)),
                    ("%s.xfs.with.it" % self.node_uuid, (stub_time, 59343945)),
                    ("%s.swift.recon.party" % self.node_uuid, (stub_time + 7, 4839)),
                    (
                        "%s.swift.recon.time" % self.node_uuid,
                        (stub_time - 1, stub_time),
                    ),
                    (
                        "%s.replication.d1.accounts.primary" % self.node_uuid,
                        (stub_time, 1),
                    ),
                    (
                        "%s.replication.d1.accounts.handoff" % self.node_uuid,
                        (stub_time, 2),
                    ),
                    (
                        "%s.replication.d1.containers.primary" % self.node_uuid,
                        (stub_time, 3),
                    ),
                    (
                        "%s.replication.d1.containers.handoff" % self.node_uuid,
                        (stub_time, 4),
                    ),
                    (
                        "%s.replication.d1.objects.primary" % self.node_uuid,
                        (stub_time, 5),
                    ),
                    (
                        "%s.replication.d1.objects.handoff" % self.node_uuid,
                        (stub_time, 6),
                    ),
                ]
            ],
            got_metrics,
        )

        # The hi-res time is int()'ed before being passed into the collectors
        self.assertListEqual(
            [mock.call(self.data, stub_time_int)], network_stats.mock_calls
        )
        self.assertListEqual(
            [mock.call(self.data, stub_time_int)], xfs_stats.mock_calls
        )
        self.assertListEqual(
            [mock.call(self.data, stub_time_int)], recon_stats.mock_calls
        )

    def test_recon_stats_default_cache_dir(self):
        argspec = inspect.getargspec(collectd_plugin.recon_stats)
        self.assertEqual(
            (["data", "timestamp", "cache_dir"], None, None, ("/var/cache/swift",)),
            argspec,
        )

    def test_recon_stats_old_swift(self):
        with open(os.path.join(self.tempdir, "account.recon"), "wb") as afp:
            afp.write(
                json.dumps(
                    {
                        "replication_stats": {
                            "no_change": 12,
                            "attempted": 6,
                            "ts_repl": 3,
                            "remote_merge": 2,
                            "failure": 1,
                            "diff": 21,
                            "rsync": 19,
                            "success": 13,
                            "remove": 14,
                            "diff_capped": 7,
                            "start": 1380221451.92608,
                            "hashmatch": 5,
                            "empty": 4,
                        },
                        "replication_time": 0.0981740951538086,
                        "account_audits_since": 1380224610.071976,
                        "account_audits_passed": 12,
                        "account_audits_failed": 0,
                        "account_auditor_pass_completed": 0.010714054107666016,
                    }
                ).encode("utf-8")
            )
        with open(os.path.join(self.tempdir, "container.recon"), "wb") as cfp:
            cfp.write(
                json.dumps(
                    {
                        "replication_stats": {
                            "no_change": 16,
                            "attempted": 8,
                            "ts_repl": 4,
                            "remote_merge": 2,
                            "failure": 1,
                            "diff": 3,
                            "rsync": 6,
                            "success": 12,
                            "remove": 24,
                            "diff_capped": 48,
                            "start": 1380221432.501042,
                            "hashmatch": 7,
                            "empty": 5,
                        },
                        "container_updater_sweep": 0.25456881523132324,
                        "replication_time": 0.06824707984924316,
                        "container_audits_passed": 16,
                        "container_audits_failed": 0,
                        "container_audits_since": 1380225328.220907,
                        "container_auditor_pass_completed": 0.02747488021850586,
                    }
                ).encode("utf-8")
            )
        with open(os.path.join(self.tempdir, "object.recon"), "wb") as ofp:
            ofp.write(
                json.dumps(
                    {
                        "object_replication_time": 0.13569668531417847,  # minutes!
                        "async_pending": 42,
                        "object_updater_sweep": 0.04616594314575195,
                    }
                ).encode("utf-8")
            )

        self.assertEqual(
            {
                "account": {
                    "replication_duration": 0.0981740951538086,
                    # start + replication_time
                    "replication_last": 1380221451.92608 + 0.098174095153808,
                    "auditor_duration": 0.010714054107666016,
                },
                "container": {
                    "replication_duration": 0.06824707984924316,
                    # start + replication_time
                    "replication_last": 1380221432.501042 + 0.06824707984924316,
                    "auditor_duration": 0.02747488021850586,
                    "updater_duration": 0.25456881523132324,
                },
                "object": {
                    "async_pending": 42,
                    "replication_duration": 0.13569668531417847 * 60,
                    # Provide the latest time at which the last full replication
                    # run could possibly have completed at.
                    "replication_last": 1380228784 - (0.13569668531417847 * 60),
                    "updater_duration": 0.04616594314575195,
                },
            },
            collectd_plugin.normalized_recon_data(1380228784, cache_dir=self.tempdir),
        )

        self.assertEqual(
            [
                # NOTE: replication_duration/replication_last are now collected
                # using a fingerprint message, not through carbon/whisper.
                ("recon.account.auditor_duration", (1380228784, 0.010714054107666016)),
                ("recon.container.auditor_duration", (1380228784, 0.02747488021850586)),
                ("recon.container.updater_duration", (1380228784, 0.25456881523132324)),
                ("recon.object.async_pending", (1380228784, 42)),
                ("recon.object.updater_duration", (1380228784, 0.04616594314575195)),
            ],
            collectd_plugin.recon_stats(self.data, 1380228784, cache_dir=self.tempdir),
        )

    def test_recon_stats_newer_swift(self):
        with open(os.path.join(self.tempdir, "account.recon"), "wb") as afp:
            afp.write(
                json.dumps(
                    {
                        "replication_stats": {
                            "no_change": 12,
                            "attempted": 6,
                            "ts_repl": 3,
                            "remote_merge": 2,
                            "failure": 1,
                            "diff": 21,
                            "rsync": 19,
                            "success": 13,
                            "remove": 14,
                            "diff_capped": 7,
                            "start": 1380221451.92608,
                            "hashmatch": 5,
                            "empty": 4,
                        },
                        "replication_time": 0.0981740951538086,  # seconds
                        "replication_last": 1380233365.809194,
                        "account_audits_since": 1380228210.269836,
                        "account_audits_passed": 12,
                        "account_audits_failed": 0,
                        "account_auditor_pass_completed": 0.006899118423461914,
                    }
                ).encode("utf-8")
            )
        with open(os.path.join(self.tempdir, "container.recon"), "wb") as cfp:
            cfp.write(
                json.dumps(
                    {
                        "replication_stats": {
                            "no_change": 16,
                            "attempted": 8,
                            "ts_repl": 4,
                            "remote_merge": 2,
                            "failure": 1,
                            "diff": 3,
                            "rsync": 6,
                            "success": 12,
                            "remove": 24,
                            "diff_capped": 48,
                            "start": 1380221432.501042,
                            "hashmatch": 7,
                            "empty": 5,
                        },
                        "container_updater_sweep": 0.25456881523132324,
                        "replication_time": 0.06824707984924316,  # seconds
                        "replication_last": 1380233362.596543,  # last completion
                        "container_audits_passed": 16,
                        "container_audits_failed": 0,
                        "container_audits_since": 1380225328.220907,
                        "container_auditor_pass_completed": 0.02747488021850586,
                    }
                ).encode("utf-8")
            )
        with open(os.path.join(self.tempdir, "object.recon"), "wb") as ofp:
            ofp.write(
                json.dumps(
                    {
                        "object_replication_time": 0.13569668531417847,  # minutes!
                        "object_replication_last": 1380233375.494914,
                        "async_pending": 42,
                        "object_updater_sweep": 0.04616594314575195,  # seconds
                        # We don't bother to collect object auditor stats because
                        # they seem a bit unreliable.  For instance, "start_time" is
                        # actually only the time since stats were last output, not the
                        # start of the current audit sweep.  And AFAICT, stats won't
                        # even get output unless the total sweep time is > 1 hr.
                        "object_auditor_stats_ALL": {
                            "audit_time": 73.19667983055115,
                            "bytes_processed": 12545977,
                            "errors": 34,
                            "passes": 1470,
                            "quarantined": 17,
                            "start_time": 1380557981.751367,
                        },
                    }
                ).encode("utf-8")
            )

        self.assertEqual(
            {
                "account": {
                    "replication_duration": 0.0981740951538086,
                    # from replication_last, not replication_stats.start
                    "replication_last": 1380233365.809194,
                    "auditor_duration": 0.006899118423461914,
                },
                "container": {
                    "replication_duration": 0.06824707984924316,
                    # from replication_last, not replication_stats.start
                    "replication_last": 1380233362.596543,
                    "auditor_duration": 0.02747488021850586,
                    "updater_duration": 0.25456881523132324,
                },
                "object": {
                    "async_pending": 42,
                    "replication_duration": 0.13569668531417847 * 60,
                    "replication_last": 1380233375.494914,
                    "updater_duration": 0.04616594314575195,
                },
            },
            collectd_plugin.normalized_recon_data(1380228784, cache_dir=self.tempdir),
        )

        self.assertEqual(
            [
                # NOTE: replication_duration/replication_last are now collected
                # using a fingerprint message, not through carbon/whisper.
                ("recon.account.auditor_duration", (1380228784, 0.006899118423461914)),
                ("recon.container.auditor_duration", (1380228784, 0.02747488021850586)),
                ("recon.container.updater_duration", (1380228784, 0.25456881523132324)),
                ("recon.object.async_pending", (1380228784, 42)),
                ("recon.object.updater_duration", (1380228784, 0.04616594314575195)),
            ],
            collectd_plugin.recon_stats(self.data, 1380228784, cache_dir=self.tempdir),
        )

    def test_recon_stats_ec_swift(self):
        with open(os.path.join(self.tempdir, "object.recon"), "wb") as ofp:
            ofp.write(
                json.dumps(
                    {
                        "object_replication_time": 0.13569668531417847,  # minutes!
                        "object_replication_last": 1380233375.494914,
                        "object_reconstruction_time": 0.28092991511027016,  # minutes!
                        "object_reconstruction_last": 1481064466.128906,
                        "async_pending": 42,
                        "object_updater_sweep": 0.04616594314575195,  # seconds
                        # We don't bother to collect object auditor stats because
                        # they seem a bit unreliable.  For instance, "start_time" is
                        # actually only the time since stats were last output, not the
                        # start of the current audit sweep.  And AFAICT, stats won't
                        # even get output unless the total sweep time is > 1 hr.
                        "object_auditor_stats_ALL": {
                            "audit_time": 73.19667983055115,
                            "bytes_processed": 12545977,
                            "errors": 34,
                            "passes": 1470,
                            "quarantined": 17,
                            "start_time": 1380557981.751367,
                        },
                    }
                ).encode("utf-8")
            )

        self.assertEqual(
            {
                "account": {},
                "container": {},
                "object": {
                    "async_pending": 42,
                    "replication_duration": 0.13569668531417847 * 60,
                    "replication_last": 1380233375.494914,
                    "reconstruction_duration": 0.28092991511027016 * 60,
                    "reconstruction_last": 1481064466.128906,
                    "updater_duration": 0.04616594314575195,
                },
            },
            collectd_plugin.normalized_recon_data(1380228784, cache_dir=self.tempdir),
        )

        self.assertEqual(
            [
                # NOTE: replication_duration/replication_last are now collected
                # using a fingerprint message, not through carbon/whisper.
                # reconstruction_duration/reconstruction_last were *never*
                # collected through carbon/whisper
                ("recon.object.async_pending", (1380228784, 42)),
                ("recon.object.updater_duration", (1380228784, 0.04616594314575195)),
            ],
            collectd_plugin.recon_stats(self.data, 1380228784, cache_dir=self.tempdir),
        )

    def test_recon_stats_no_files(self):
        self.assertEqual(
            [],
            collectd_plugin.recon_stats(self.data, 1380228784, cache_dir=self.tempdir),
        )

    def test_recon_stats_bad_json(self):
        with open(os.path.join(self.tempdir, "account.recon"), "wb") as afp:
            afp.write("slap-happy".encode("utf-8"))
        with open(os.path.join(self.tempdir, "container.recon"), "wb") as cfp:
            cfp.write("slim-shady".encode("utf-8"))
        with open(os.path.join(self.tempdir, "object.recon"), "wb") as ofp:
            ofp.write("slip-slop".encode("utf-8"))

        self.assertEqual(
            [],
            collectd_plugin.recon_stats(self.data, 1380228784, cache_dir=self.tempdir),
        )

    def test_recon_stats_no_keys(self):
        with open(os.path.join(self.tempdir, "account.recon"), "wb") as afp:
            afp.write(json.dumps({}).encode("utf-8"))
        with open(os.path.join(self.tempdir, "container.recon"), "wb") as cfp:
            cfp.write(json.dumps({}).encode("utf-8"))
        with open(os.path.join(self.tempdir, "object.recon"), "wb") as ofp:
            ofp.write(json.dumps({}).encode("utf-8"))

        self.assertEqual(
            [],
            collectd_plugin.recon_stats(self.data, 1380228784, cache_dir=self.tempdir),
        )

    @contextlib.contextmanager
    def _patch_stats_file(self, stub=None):
        self.stats_file = os.path.join(self.tempdir, "replication_progress.json")
        if stub is not None:
            with open(self.stats_file, "w") as f:
                f.write(stub)
        with mock.patch(
            "collectd_plugin.collectd_plugin.REPLICATION_PROGRESS_STATS_FILE", self.stats_file
        ):
            yield

    def test_read_replication_progress(self):
        # test no file
        with self._patch_stats_file():
            stats = collectd_plugin.read_replication_progress()
        self.assertEqual(stats, {})
        self.assertFalse(os.path.exists(self.stats_file))
        # test empty file
        with self._patch_stats_file(stub=""):
            stats = collectd_plugin.read_replication_progress()
        self.assertEqual(stats, {})
        self.assertEqual(open(self.stats_file).read(), "")
        # test with stub data
        stub = {
            "d1": {
                "accounts": {"primary": 1, "handoff": 2},
                "containers": {"primary": 3, "handoff": 4},
                "objects": {"primary": 5, "handoff": 6},
            },
            "d2": {
                "accounts": {"primary": 7, "handoff": 8},
                "containers": {"primary": 9, "handoff": 10},
                "objects": {"primary": 11, "handoff": 12},
            },
        }
        # test with stub data
        with self._patch_stats_file(stub=json.dumps(stub)):
            stats = collectd_plugin.read_replication_progress()
        self.assertEqual(stats, stub)

    def test_replication_progress_stats(self):
        d = {}
        t = 1462999369
        # test no file
        with self._patch_stats_file():
            metrics = collectd_plugin.replication_progress_stats(d, t)
        self.assertEqual(metrics, [])

        # test empty file
        with self._patch_stats_file(stub=""):
            metrics = collectd_plugin.replication_progress_stats(d, t)
        self.assertEqual(metrics, [])

        # test stub data
        stub = {
            "d1": {
                "accounts": {"primary": 1, "handoff": 2},
                "containers": {"primary": 3, "handoff": 4},
                "objects": {"primary": 5, "handoff": 6},
            },
            "d2": {
                "accounts": {"primary": 7, "handoff": 8},
                "containers": {"primary": 9, "handoff": 10},
                "objects": {"primary": 11, "handoff": 12},
            },
            "d3": {},
        }
        with self._patch_stats_file(stub=json.dumps(stub)):
            metrics = collectd_plugin.replication_progress_stats(d, t)
        expected = [
            ("replication.d1.accounts.primary", (t, 1)),
            ("replication.d1.accounts.handoff", (t, 2)),
            ("replication.d1.containers.primary", (t, 3)),
            ("replication.d1.containers.handoff", (t, 4)),
            ("replication.d1.objects.primary", (t, 5)),
            ("replication.d1.objects.handoff", (t, 6)),
            ("replication.d2.accounts.primary", (t, 7)),
            ("replication.d2.accounts.handoff", (t, 8)),
            ("replication.d2.containers.primary", (t, 9)),
            ("replication.d2.containers.handoff", (t, 10)),
            ("replication.d2.objects.primary", (t, 11)),
            ("replication.d2.objects.handoff", (t, 12)),
            ("replication.ALL.accounts.primary", (t, 8)),
            ("replication.ALL.accounts.handoff", (t, 10)),
            ("replication.ALL.containers.primary", (t, 12)),
            ("replication.ALL.containers.handoff", (t, 14)),
            ("replication.ALL.objects.primary", (t, 16)),
            ("replication.ALL.objects.handoff", (t, 18)),
        ]
        self.assertEqual(set(metrics), set(expected))
