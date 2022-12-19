# Copyright (c) 2011-2020, NVIDIA CORPORATION.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA or see
# <http://www.gnu.org/licenses/>.

from collections import defaultdict
from datetime import datetime
import errno
import json
import os
from pystatsd import Server
import re
from six.moves.queue import Queue, Empty
from six.moves import cPickle as pickle
from six.moves import range
from six.moves.urllib.parse import urlsplit, urlunsplit
import socket
import struct
import sys
import threading
from time import time, sleep
import traceback


running_inside_collectd = False
try:
    import collectd

    running_inside_collectd = True
except ImportError:
    pass


# Work around a threading bug present in (at least) 2.7.3
# This hack appears safe for 2.7.6 (which has this fixed).
# See https://bugs.python.org/msg160297
threading._DummyThread._Thread__stop = lambda x: 42


def escape_metric_segment(segment):
    """
    Escape a string that needs to be a single Whisper metric segment.
    """
    return segment.replace(".", "_")


# The second argument here is just to work around flexmock's
# deficiencies.
def resolve_hostname_in_url(url, resolve=socket.gethostbyname):
    parsed = urlsplit(url)
    hostname = parsed.hostname

    if _looks_like_ipv4(hostname):
        ip = hostname
    elif _looks_like_ipv6(hostname):
        ip = "[%s]" % hostname
    else:
        ip = resolve(hostname)

    if parsed.port is not None:
        new_netloc = "%s:%s" % (ip, parsed.port)
    else:
        new_netloc = ip

    return urlunsplit(
        (parsed.scheme, new_netloc, parsed.path, parsed.query, parsed.fragment)
    )


def _looks_like_ipv4(maybe_ip):
    try:
        socket.inet_pton(socket.AF_INET, maybe_ip)
        return True
    except socket.error:
        return False


def _looks_like_ipv6(maybe_ip):
    try:
        socket.inet_pton(socket.AF_INET6, maybe_ip)
        return True
    except socket.error:
        return False


db = {}

PING_TIMEOUT = 15  # seconds
LINGER_PERIOD = 1800  # half an hour
WRITE_BATCH_THRESHOLD = 2  # no metrics for X seconds? send batch.


def _log(msg, *args):
    # Poor-man's logger which just goes to stderr
    # (special-case escaping for messages with no args that contain percent)
    if not args:
        msg = msg.replace("%", "%%")
    msg = "[%%s] %s\n" % (msg,)
    msg_args = [datetime.now().isoformat(" ")]
    msg_args.extend(args)
    sys.stderr.write(msg % tuple(msg_args))


def _seq_chunker(seq, size):
    return (seq[pos : pos + size] for pos in range(0, len(seq), size))


def init():
    # Populate db with types.db data (DSes)
    types_db = open("/opt/ss/share/collectd/types.db")
    line = types_db.readline()
    while line:
        matches = re.findall(r"(?:^(\w+)|\s+(\w+):\S+,?)", line)
        if len(matches) > 1:
            match_list = []
            for match in matches[1:]:
                match_list.append(match[1])
            db[matches[0][0]] = match_list
        line = types_db.readline()
    types_db.close()


def config(config_obj, data):
    """Store configuration data like the zmq socket endpoint."""
    try:
        for sub_obj in config_obj.children:
            data[sub_obj.key] = sub_obj.values[0]
        data["statsd_flush_interval"] = int(data["statsd_flush_interval"])
        if "statsd_pct_thresholds" in data:
            data["statsd_pct_thresholds"] = [
                float(s) for s in data["statsd_pct_thresholds"].split()
            ]
        elif "statsd_pct_threshold" in data:
            data["statsd_pct_thresholds"] = [float(data["statsd_pct_threshold"])]

        # We're piggy-backing initialization in config(), which is mixing
        # semantics, but init() doesn't have access to our opaque callback
        # "data" dict.
        data["metrics_queue"] = Queue()

        data["threads"] = []

        if data.get("monitor_endpoint"):
            if data.get("ping_endpoint"):
                pinger = threading.Thread(
                    name="collectd pinger",
                    target=zmq_pinger_thread,
                    args=(data["ping_endpoint"], data["node_uuid"]),
                )
                pinger.daemon = True  # must set before .start()
                pinger.start()
                data["threads"].append(pinger)

            writer = threading.Thread(
                target=zmq_writer_thread,
                args=(
                    data["metrics_queue"],
                    data["monitor_endpoint"],
                    data["node_uuid"],
                ),
            )
            writer.daemon = True  # must set before .start()
            writer.start()
            data["threads"].append(writer)
        elif data.get("carbon_host") and data.get("carbon_port"):
            writer = threading.Thread(
                name="collectd carbon writer",
                target=carbon_writer_thread,
                args=(data["metrics_queue"], data["carbon_host"], data["carbon_port"]),
            )
            writer.daemon = True
            writer.start()
            data["threads"].append(writer)

        if "statsd_bind_ip" in data:
            statsd = threading.Thread(
                name="statsd",
                target=statsd_thread,
                args=(
                    data["metrics_queue"],
                    data["statsd_bind_ip"],
                    data["statsd_port"],
                    data["statsd_pct_thresholds"],
                    data["statsd_flush_interval"],
                    data["node_uuid"],
                    data.get("statsd_forward_host"),
                    data.get("statsd_forward_port"),
                    data.get("statsd_forward_prefix_hostname"),
                ),
            )
            statsd.daemon = True
            statsd.start()
            data["threads"].append(statsd)

    except Exception as e:
        _log("Error in config: %r", e)
        os._exit(2)


def statsd_thread(
    metrics_queue,
    bind_ip,
    port,
    pct_thresholds,
    flush_interval_seconds,
    node_uuid,
    statsd_forward_host,
    statsd_forward_port,
    statsd_forward_prefix_hostname,
):
    if statsd_forward_host and statsd_forward_port:
        _log(
            "Forwarding StatsD stats to %s:%s", statsd_forward_host, statsd_forward_port
        )
    statsd_forward_prefix = ""
    if statsd_forward_prefix_hostname:
        statsd_forward_prefix = socket.gethostname()
        _log("Forwarding StatsD stats with hostname prefix %r", statsd_forward_prefix)

    if data.get("monitor_endpoint") or (
            data.get("carbon_host") and data.get("carbon_port")):
        transport = "graphite_queue"
    else:
        # force our hacked-up pystatsd Server to use TransportNop
        transport = "graphite"

    server = Server(
        pct_thresholds=pct_thresholds,
        transport=transport,
        queue=metrics_queue,
        flush_interval=flush_interval_seconds,
        counters_prefix=node_uuid,
        timers_prefix=node_uuid,
        statsd_forward_host=statsd_forward_host,
        statsd_forward_port=statsd_forward_port,
        statsd_forward_prefix=statsd_forward_prefix,
    )
    server.serve(hostname=bind_ip, port=int(port))


def zmq_pinger_thread(ping_endpoint, node_uuid):
    zmq = 1  # appease pyflakes which can't understand the following import
    exec("import zmq")

    context = zmq.Context.instance()
    while True:
        endpoint = resolve_hostname_in_url(ping_endpoint)
        ping_socket = context.socket(zmq.REQ)
        ping_socket.setsockopt(zmq.LINGER, 0)
        ping_socket.connect(endpoint)

        ping_poller = zmq.Poller()
        ping_poller.register(ping_socket, zmq.POLLIN)
        ping_socket.send("PING:%s" % (node_uuid,))

        # wait for a PONG response
        socks = dict(ping_poller.poll(1000 * PING_TIMEOUT))
        if socks.get(ping_socket) == zmq.POLLIN:
            # loop to soak any extra PONGs just in case
            while socks.get(ping_socket) == zmq.POLLIN:
                pong = ping_socket.recv()
                if pong != "PONG":
                    _log("Received unexpected PING response: %r", pong)
                socks = dict(ping_poller.poll(0))
        else:
            # Failed to receive a PONG within timeout
            _log("Failed to receive a PONG; exiting!")
            os._exit(3)
        ping_socket.close()
        sleep(10)


def zmq_writer_thread(metrics_queue, monitor_endpoint, node_uuid):
    zmq = 1  # appease pyflakes which can't understand the following import
    exec("import zmq")

    context = zmq.Context.instance()
    sock = None
    while True:
        metrics = metrics_queue.get()  # blocking
        # Don't let more than 5x WRITE_BATCH_THRESHOLD seconds go by without a
        # flush
        start = time()
        while True and time() - start < 5 * WRITE_BATCH_THRESHOLD:
            try:
                additional_metrics = metrics_queue.get(timeout=WRITE_BATCH_THRESHOLD)
                metrics.extend(additional_metrics)
            except Empty:
                break
        if sock:
            sock.close()

        endpoint = resolve_hostname_in_url(monitor_endpoint)
        sock = context.socket(zmq.PUSH)
        # Setting a linger time is only meant as a brake on memory
        # usage in weird network situations.
        #
        # In normal good times, pings and monitoring data both work,
        # so data doesn't sit around for more than a couple RTTs
        # unacknowledged, and this setting does nothing.
        #
        # In normal bad times, pings and monitoring data both fail, so
        # after a failed ping, we call os._exit() and our process goes
        # kaput immediately, so the monitoring data doesn't accumulate
        # in memory.
        #
        # However, on bizarro-net, pings get through, but monitoring
        # data doesn't. In that case, up to $LINGER_PERIOD worth of
        # monitoring data will queue up, in memory, waiting to be
        # shipped out. If we didn't set the linger time for the
        # monitoring data, then since the default linger time is 0
        # (i.e. keep data forever), that would be a memory leak.
        sock.setsockopt(zmq.LINGER, LINGER_PERIOD * 1000)
        sock.connect(endpoint)
        # Send out at most 150 metrics in a single batch
        for metrics_chunk in _seq_chunker(metrics, 150):
            pickled = pickle.dumps(metrics_chunk)
            payload = (
                struct.pack("!LL", 8 + len(node_uuid) + len(pickled), len(node_uuid))
                + node_uuid
                + pickled
            )
            sock.send(payload)


def carbon_writer_thread(metrics_queue, carbon_host, carbon_port):
    carbon_receiver = (carbon_host, int(carbon_port))
    carbon_sock = None
    while True:
        try:
            if not carbon_sock:
                _log("Connecting to carbon at %s:%d", *carbon_receiver)
                carbon_sock = socket.create_connection(carbon_receiver)
                _log("Connected to carbon at %s:%d", *carbon_receiver)
            metrics = metrics_queue.get()  # blocking
            for metric in metrics:
                m_name = metric[0]
                datapoints = metric[1:]
                for datapoint in datapoints:
                    m_time = datapoint[0]
                    m_val = datapoint[1]
                    carbon_sock.sendall("%s %s %s\n" % (m_name, m_val, m_time))
        except socket.error as sockerr:
            _log("Something happened to my carbon socket: %r", sockerr)
            carbon_sock = None
            sleep(0.05)  # don't peg the CPU if carbon is down


def write(values, data):
    """Pass collected data to the writer thread.  Also catch thread death."""
    if not all(t.isAlive() for t in data["threads"]):
        # In general, suicide is not the answer.  Here, we make an exception.
        os._exit(4)

    # Pickled datapoints look like this:
    # [
    #   (<metric_string>, (<time>, <value>))
    # ]
    # so the fact that they wait around a while does not affect their
    # correctness.
    data["metrics_queue"].put(
        [
            # We cast to float since we saw some metrics with values like '0' (a
            # one-character string).  It only broke the metric diversion code,
            # which has been fixed, but we should make sure we're sending only
            # float values out of here, too.
            (metric, (values.time, float(v)))
            for metric, v in split_values(values, data)
        ]
    )


def read(data):
    """
    Collect some metrics and send 'em directly to the writer thread.

    The node_uuid is prepended to metric names generated by the actual
    collection function(s).

    Pickled datapoints look like this:
    [
      (<metric_string>, (<time>, <value>))
    ]
    so the fact that they wait around a while does not affect their
    correctness.
    """

    t = int(time())
    metrics = []
    try:
        # Don't let otherwise-untrapped exceptions (code bugs) in the
        # individual collectors prevent us from sending metrics we did manage
        # to get.
        metrics.extend(network_stats(data, t))
        metrics.extend(xfs_stats(data, t))
        metrics.extend(recon_stats(data, t))
        metrics.extend(replication_progress_stats(data, t))
    except Exception:
        _log(traceback.format_exc())

    # Prepend the node_uuid once, to make the actual collector function(s)
    # simpler.
    if data["node_uuid"]:
        metrics = [(data["node_uuid"] + "." + m, tv) for m, tv in metrics]

    data["metrics_queue"].put(metrics)


def normalized_sharding_data(cache_dir="/var/cache/swift"):
    container_file = os.path.join(cache_dir, "container.recon")
    try:
        with open(container_file, "r") as fp:
            data = json.load(fp)
    except (OSError, IOError, ValueError):
        # Missing file, bad perms, JSON decode error...
        return {}
    sharding_info = data.get("sharding_stats", {}).get("sharding", {})
    # If present, sharding_candidates should be of the form
    # {
    #   "found": 1234,
    #   "top": [
    #      {"account": "AUTH_foo",
    #       "container": "c",
    #       "file_size": 1234,
    #       "meta_timestamp": "1522952237.71391"
    #       "node_index": 0,
    #       "object_count": 1049,
    #       "path": "/srv/node/d2/containers/.../hash.db",
    #       "root": "AUTH_a/c2",
    #      },
    #      ...
    #   ]
    # }
    # where 'top' is limited by recon_candidates_limit. shrinking_candidates
    # doesn't actually exist yet, but presumably will be somewhat similar.
    #
    # sharding_in_progress, meanwhile, should be of the form
    # {
    #   "all": [
    #      {"account": "AUTH_foo",
    #       "container": "c",
    #       "file_size": 1234,
    #       "meta_timestamp": "1522952237.71391"
    #       "node_index": 0,
    #       "object_count": 1049,
    #       "path": "/srv/node/d2/containers/.../hash.db",
    #       "root": "AUTH_a/c2",
    #       "error": null or string,
    #       "state": "sharded" (or whatever),
    #       "db_state": "sharding" (or whatever),
    #       "found": x,
    #       "cleaved": y,
    #       "active": z,
    #      },
    #      ...
    #   ]
    # }
    return {
        k: sharding_info.get(k, {})
        for k in (
            "sharding_candidates",
            "sharding_in_progress",
            "shrinking_candidates",
        )
    }


def normalized_recon_data(now, cache_dir="/var/cache/swift"):
    account_file = os.path.join(cache_dir, "account.recon")
    container_file = os.path.join(cache_dir, "container.recon")
    object_file = os.path.join(cache_dir, "object.recon")

    data = {"account": {}, "container": {}, "object": {}}

    if os.path.exists(account_file):
        try:
            account_stats = json.loads(open(account_file).read())
            if "replication_time" in account_stats:
                data["account"]["replication_duration"] = account_stats[
                    "replication_time"
                ]
            if "replication_last" in account_stats:
                data["account"]["replication_last"] = account_stats["replication_last"]
            elif (
                "replication_stats" in account_stats
                and "replication_time" in account_stats
            ):
                if "start" in account_stats["replication_stats"]:
                    data["account"]["replication_last"] = (
                        account_stats["replication_stats"]["start"]
                        + account_stats["replication_time"]
                    )
            if "account_auditor_pass_completed" in account_stats:
                data["account"]["auditor_duration"] = account_stats[
                    "account_auditor_pass_completed"
                ]
        except (ValueError, OSError):
            pass  # JSON decode error

    if os.path.exists(container_file):
        try:
            container_stats = json.loads(open(container_file).read())
            if "replication_time" in container_stats:
                data["container"]["replication_duration"] = container_stats[
                    "replication_time"
                ]
            if "replication_last" in container_stats:
                data["container"]["replication_last"] = container_stats[
                    "replication_last"
                ]
            elif (
                "replication_stats" in container_stats
                and "replication_time" in container_stats
            ):
                if "start" in container_stats["replication_stats"]:
                    data["container"]["replication_last"] = (
                        container_stats["replication_stats"]["start"]
                        + container_stats["replication_time"]
                    )
            if "sharding_time" in container_stats:
                data["container"]["sharding_duration"] = container_stats[
                    "sharding_time"
                ]
            if "sharding_last" in container_stats:
                data["container"]["sharding_last"] = container_stats["sharding_last"]
            if "container_auditor_pass_completed" in container_stats:
                data["container"]["auditor_duration"] = container_stats[
                    "container_auditor_pass_completed"
                ]
            if "container_updater_sweep" in container_stats:
                data["container"]["updater_duration"] = container_stats[
                    "container_updater_sweep"
                ]
        except (ValueError, OSError):
            pass  # JSON decode error

    if os.path.exists(object_file):
        try:
            with open(object_file) as fp:
                object_stats = json.load(fp)
            if "async_pending" in object_stats:
                data["object"]["async_pending"] = object_stats["async_pending"]
            if "object_replication_time" in object_stats:
                # normalize to seconds
                data["object"]["replication_duration"] = (
                    object_stats["object_replication_time"] * 60
                )
            if "object_replication_last" in object_stats:
                data["object"]["replication_last"] = object_stats[
                    "object_replication_last"
                ]
            elif "object_replication_time" in object_stats:
                data["object"]["replication_last"] = (
                    now - object_stats["object_replication_time"] * 60
                )
            if "object_reconstruction_time" in object_stats:
                # normalize to seconds
                data["object"]["reconstruction_duration"] = (
                    object_stats["object_reconstruction_time"] * 60
                )
            if "object_reconstruction_last" in object_stats:
                data["object"]["reconstruction_last"] = object_stats[
                    "object_reconstruction_last"
                ]
            if "object_updater_sweep" in object_stats:
                data["object"]["updater_duration"] = object_stats[
                    "object_updater_sweep"
                ]
        except (ValueError, OSError):
            pass  # JSON decode error

    return data


def recon_stats(data, timestamp, cache_dir="/var/cache/swift"):
    metrics = []
    recon_data = normalized_recon_data(timestamp, cache_dir)

    account_stats = recon_data["account"]
    # NOTE: replication_duration/replication_last no longer tracked using
    # Whisper metrics.
    if "auditor_duration" in account_stats:
        metrics.append(
            (
                "recon.account.auditor_duration",
                (timestamp, account_stats["auditor_duration"]),
            )
        )

    container_stats = recon_data["container"]
    # NOTE: replication_duration/replication_last no longer tracked using
    # Whisper metrics.
    if "auditor_duration" in container_stats:
        metrics.append(
            (
                "recon.container.auditor_duration",
                (timestamp, container_stats["auditor_duration"]),
            )
        )
    if "updater_duration" in container_stats:
        metrics.append(
            (
                "recon.container.updater_duration",
                (timestamp, container_stats["updater_duration"]),
            )
        )

    object_stats = recon_data["object"]
    if "async_pending" in object_stats:
        metrics.append(
            ("recon.object.async_pending", (timestamp, object_stats["async_pending"]))
        )
    # NOTE: replication_duration/replication_last no longer tracked using
    # Whisper metrics.
    if "updater_duration" in object_stats:
        metrics.append(
            (
                "recon.object.updater_duration",
                (timestamp, object_stats["updater_duration"]),
            )
        )

    return metrics


REPLICATION_PROGRESS_STATS_FILE = "/opt/ss/var/lib/replication_progress.json"


def read_replication_progress():
    """
    Read the stats file from disk.
    """
    try:
        with open(REPLICATION_PROGRESS_STATS_FILE) as f:
            return json.load(f)
    except (OSError, IOError) as e:
        if e.errno not in (errno.ENOENT,):
            raise
    except ValueError:
        pass
    return {}


def replication_progress_stats(data, timestamp):
    metrics = []
    stats = read_replication_progress()
    aggregate = defaultdict(lambda: defaultdict(int))
    for device, type_stats in stats.items():
        for type_, stats in type_stats.items():
            prefix = "replication.%s.%s." % (device, type_)
            for key, value in stats.items():
                metrics.append((prefix + key, (timestamp, value)))
                aggregate[type_][key] += value
    for type_, stats in aggregate.items():
        prefix = "replication.ALL.%s." % type_
        for key, value in stats.items():
            metrics.append((prefix + key, (timestamp, value)))
    return metrics


def network_stats(data, timestamp):
    """Collect some network stats and return them in the format:
        [(<metric_name>, (timestamp, <value>))]
    """
    stats_file = "/proc/net/snmp"
    parsed = {}
    if os.path.exists(stats_file):
        try:
            with open(stats_file, "rb") as stats_fh:
                line_type, labels = None, None
                for line in stats_fh.readlines():
                    parts = line.split()
                    if parts[1].isdigit() and parts[0] == line_type + ":":
                        parsed[line_type] = dict(zip(labels, parts[1:]))
                    elif not parts[1].isdigit():
                        line_type = parts[0].split(":")[0]
                        labels = parts[1:]
        except Exception:
            _log(traceback.format_exc())
            return []
    else:
        return []

    return [
        ("net.udp.InErrors", (timestamp, parsed["Udp"]["InErrors"])),
        ("net.udp.RcvbufErrors", (timestamp, parsed["Udp"]["RcvbufErrors"])),
        ("net.tcp.AttemptFails", (timestamp, parsed["Tcp"]["AttemptFails"])),
        ("net.tcp.RetransSegs", (timestamp, parsed["Tcp"]["RetransSegs"])),
        ("net.ip.InHdrErrors", (timestamp, parsed["Ip"]["InHdrErrors"])),
        ("net.ip.FragFails", (timestamp, parsed["Ip"]["FragFails"])),
        ("net.ip.FragCreates", (timestamp, parsed["Ip"]["FragCreates"])),
    ]


def xfs_stats(data, timestamp):
    """
    Collect some xfs stats and return them in the format:
        [(<metric_name>, (timestamp, <value>))]

    See http://xfs.org/index.php/Runtime_Stats
    """
    stats_file = "/proc/fs/xfs/stat"
    parsed = {}
    if os.path.exists(stats_file):
        try:
            with open(stats_file, "rb") as stats_fh:
                for line in stats_fh.readlines():
                    label, stats = line.split(" ", 1)
                    parsed[label] = map(int, stats.split())
        except Exception:
            _log(traceback.format_exc())
            return []
    else:
        return []

    stats = []
    potential_stats = {
        ("dir", "dir_ops"): [
            # This is a count of the number of file name directory lookups in
            # XFS filesystems. It counts only those lookups which miss in the
            # operating system's directory name lookup cache and must search
            # the real directory structure for the name in question. The count
            # is incremented once for each level of a pathname search that
            # results in a directory lookup.
            ("lookup", 0),
            # This is the number of times the XFS directory getdents operation
            # was performed. The getdents operation is used by programs to read
            # the contents of directories in a file system independent fashion.
            # This count corresponds exactly to the number of times the
            # getdents(2) system call was successfully used on an XFS
            # directory.
            ("getdents", 3),
        ],
        ("ig", "inode_ops"): [
            # This is the number of times the operating system looked for an
            # XFS inode in the inode cache and found it. The closer this count
            # is to the ig_attempts count the better the inode cache is
            # performing.
            ("ig_found", 1),
            # This is the number of times the operating system looked for an
            # XFS inode in the inode cache and the inode was not there. The
            # further this count is from the ig_attempts count the better.
            ("ig_missed", 3),
            # This is the number of times the operating system recycled an XFS
            # inode from the inode cache in order to use the memory for that
            # inode for another purpose.
            ("ig_reclaims", 5),
        ],
        ("log", "log"): [
            # This variable counts the number of log buffer writes going to the
            # physical log partitions of all XFS filesystems.
            ("writes", 0),
            # This variable counts (in 512-byte units) the information being
            # written to the physical log partitions of all XFS filesystems.
            ("blocks", 1),
            # This variable keeps track of times when a logged transaction can
            # not get any log buffer space. When this occurs, all of the
            # internal log buffers are busy flushing their data to the physical
            # on-disk log.
            ("noiclogs", 2),
            # The number of times the in-core log is forced to disk. It is
            # equivalent to the number of successful calls to the function
            # xfs_log_force().
            ("force", 3),
        ],
        ("xstrat", "xstrat"): [
            # This is the number of buffers flushed out by the XFS flushing
            # daemons which are written to contiguous space on disk. This one
            # is GOOD.
            ("quick", 0),
            # This is the number of buffers flushed out by the XFS flushing
            # daemons which are written to non-contiguous space on disk. This
            # one is BAD.
            ("split", 1),
        ],
        ("xpc", "xpc"): [
            # This is a count of bytes of file data flushed out by the XFS
            # flushing daemons.  64-bit counter.
            ("xstrat_bytes", 0),
            # This is a count of bytes written via write(2) system calls to
            # files in XFS file systems. It can be used in conjunction with the
            # write_calls count to calculate the average size of the write
            # operations to files in XFS file systems.
            ("write_bytes", 1),
            # This is a count of bytes read via read(2) system calls to files
            # in XFS file systems. It can be used in conjunction with the
            # read_calls count to calculate the average size of the read
            # operations to files in XFS file systems.
            ("read_bytes", 2),
        ],
        ("rw", "rw"): [
            # This is the number of write(2) system calls made to files in XFS
            # file systems.
            ("write_calls", 0),
            # This is the number of read(2) system calls made to files in XFS
            # file systems.
            ("read_calls", 1),
        ],
    }
    for parsed_key, metric_group in potential_stats.iterkeys():
        if parsed_key in parsed:
            parsed_len = len(parsed[parsed_key])
            for metric_key, parsed_idx in potential_stats[(parsed_key, metric_group)]:
                if parsed_idx < parsed_len:
                    stats.append(
                        (
                            "xfs.%s.%s" % (metric_group, metric_key),
                            (timestamp, parsed[parsed_key][parsed_idx]),
                        )
                    )
    return stats


def pascal_to_snake(pascal_str):
    first_char = True
    snake_str = ""
    for char in pascal_str:
        if char.isupper():
            if first_char:
                first_char = False
                snake_str += char.lower()
            else:
                snake_str += "_%s" % char.lower()
        else:
            snake_str += char
    return snake_str


def split_values(values, data):
    return_list = []
    if len(values.values) == 1:
        # simple case
        metric = _metric(values, data)
        if metric:
            return_list.append((metric, values.values[0]))
    else:
        for ds, value in zip(db[values.type], values.values):
            metric = _metric(values, data, ds)
            if metric:
                return_list.append((metric, value))
    return return_list


def _metric(values, data, ds=None):
    """
    Generate a carbon metric string something along the lines of:
        <node_uuid>.<plugin>.<plugin_instance>.<type>.<type_instance>.<ds>

    Don't include metric parts which are empty-string.  Also suppress
    duplicates between plugin & plugin_instance and between type and plugin.

    This is also where filtering can take place, as a false-y return will
    suppress the sending of the metric.
    """

    if _should_filter(values, data, ds):
        return None

    # Compensate for the Aggregation plugin's introduced one-interval lag.
    # When Aggregation emits a data point, the value it has is *actually* for
    # one "interval" ago.
    if values.plugin == "aggregation":
        values.time -= data["statsd_flush_interval"]

    metric = ".".join(
        escape_metric_segment(part)
        for part in (
            values.plugin,
            (values.plugin_instance if values.plugin_instance != values.plugin else ""),
            (values.type if values.type != values.plugin else ""),
            values.type_instance,
            ds,
        )
        if part
    )

    # HACK: we don't want this escaped, as it lets us use things like
    # "ssman.g01" as hostnames to get all our metrics prefixed
    if data["node_uuid"]:
        metric = data["node_uuid"] + "." + metric

    return metric


FILTER = dict(
    cpu=[dict(plugin="cpu")],  # Filter all cpu metrics; we aggregate 'em
    memcached=[
        dict(type="ps_cputime"),
        dict(type="ps_count"),
        dict(type="memcached_command", type_instance="flush"),
        dict(type="memcached_command", type_instance="touch"),
        dict(type="percent", type_instance="incr_hitratio"),
        dict(type="percent", type_instance="decr_hitratio"),
        dict(type="memcached_octets"),
    ],
    df=[
        dict(plugin_instance="root", type_instance="used"),
        dict(type_instance="reserved"),
    ],
    load=[dict(ds="longterm")],
    processes=[
        dict(type="ps_stacksize"),
        dict(type="ps_pagefaults"),
        dict(type="ps_data"),
        dict(type="ps_code"),
        dict(type="ps_state"),
        # For fd count, our patched collectd also sends them as the old name
        # "ps_fd_count", so here we filter out the new name; this prevents us
        # having to migrate whisper data or combine 2 metric names.
        dict(type="file_handles"),
        # These just measure system calls doing "I/O" which could be terminals
        # or sockets--which isn't as interesting to me... so we filter them.
        dict(type="io_ops"),
        dict(type="io_octets"),
    ],
    ipvs=[dict(type="if_packets"), dict(type="if_octets")],
    swap=[
        dict(type="swap", type_instance="free"),
        dict(type="swap", type_instance="cached"),
    ],
    openvpn=[dict(type_instance="overhead"), dict(plugin_instance="UNDEF")],
    interface=[dict(plugin_instance="tun0")],
)


def _should_filter(values, data, ds):
    for f in FILTER.get(values.plugin, []):
        should_filter = True
        for k in f.keys():
            if k == "ds":
                if ds != f[k]:
                    should_filter = False
            else:
                if getattr(values, k) != f[k]:
                    should_filter = False
        if should_filter:
            return True
    return False


# Only hook in to collectd code if running inside collectd.  Otherwise it's
# really difficult to test the code in here.
if running_inside_collectd:
    data = {}
    collectd.register_init(init)
    collectd.register_config(config, data)
    if data.get("monitor_endpoint") or (
            data.get("carbon_host") and data.get("carbon_port")):
        collectd.register_write(write, data)
        collectd.register_read(read, data=data)
