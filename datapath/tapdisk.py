#!/usr/bin/env python

import os
import signal

# from python-fdsend
# import fdsend

import xapi
import image
import xapi.storage.api.volume
from xapi.storage.common import call
from xapi.storage import log
import pickle
import urlparse

# Use Xen tapdisk to create block devices from files

blktap2_prefix = "/dev/xen/blktap-2/tapdev"

nbdclient_prefix = "/var/run/blktap-control/nbdclient"
nbdserver_prefix = "/var/run/blktap-control/nbdserver"

TD_PROC_METADATA_DIR = "/var/run/nonpersistent/dp-tapdisk"
TD_PROC_METADATA_FILE = "meta.pickle"


class Tapdisk:

    def __init__(self, minor, pid, f):
        self.minor = minor
        self.pid = pid
        self.f = f
        self.secondary = None  # mirror destination

    def __repr__(self):
        return "Tapdisk(%s, %s, %s)" % (self.minor, self.pid, self.f)

    def destroy(self, dbg):
        self.pause(dbg)
        call(dbg,
             ["tap-ctl",
              "destroy",
              "-m",
              str(self.minor),
              "-p",
              str(self.pid)])

    def close(self, dbg):
        call(dbg,
             ["tap-ctl",
              "close",
              "-m",
              str(self.minor),
              "-p",
              str(self.pid)])
        self.f = None

    def open(self, dbg, f, o_direct=True):
        assert (isinstance(f, image.Vhd) or isinstance(f, image.Raw))
        args = ["tap-ctl", "open", "-m", str(self.minor),
                   "-p", str(self.pid), "-a", str(f)]
        if not o_direct:
            args.append("-D")
        call(dbg, args)
        self.f = f

    def pause(self, dbg):
        call(dbg,
             ["tap-ctl",
              "pause",
              "-m",
              str(self.minor),
              "-p",
              str(self.pid)])

    def unpause(self, dbg):
        cmd = ["tap-ctl", "unpause", "-m",
               str(self.minor), "-p", str(self.pid)]
        if self.secondary is not None:
            cmd = cmd + ["-2 ", self.secondary]
        call(dbg, cmd)

    def block_device(self):
        return blktap2_prefix + str(self.minor)

    """
    ToDo: fdsend needs to be imported
    def start_mirror(self, dbg, fd):
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(nbdclient_prefix + str(self.pid))
        token = "token"
        fdsend.sendfds(sock, token, fds=[fd])
        sock.close()
        self.secondary = "nbd:" + token
        self.pause(dbg)
        self.unpause(dbg)
    """

    def stop_mirror(self, dbg):
        self.secondary = None
        self.pause(dbg)
        self.unpause(dbg)

    """
    ToDo: fdsend needs to be imported
    def receive_nbd(self, dbg, fd):
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect("%s%d.%d" % (nbdserver_prefix, self.pid, self.minor))
        token = "token"
        fdsend.sendfds(sock, token, fds=[fd])
        sock.close()
    """


def create(dbg):
    output = call(dbg, ["tap-ctl", "spawn"]).strip()
    pid = int(output)
    output = call(dbg, ["tap-ctl", "allocate"]).strip()
    prefix = blktap2_prefix
    minor = None
    if output.startswith(prefix):
        minor = int(output[len(prefix):])
    if minor is None:
        os.kill(pid, signal.SIGQUIT)
        raise xapi.InternalError("tap-ctl allocate returned unexpected " +
                                 "output: %s" % (output))
    call(dbg, ["tap-ctl", "attach", "-m", str(minor), "-p", str(pid)])
    return Tapdisk(minor, pid, None)


def find_by_file(dbg, f):
    log.debug("%s: find_by_file f=%s" % (dbg, f))
    assert (isinstance(f, image.Path))
    # See whether this host has any metadata about this file
    try:
        log.debug("%s: find_by_file trying uri=%s" % (dbg, f.path))
        tap = load_tapdisk_metadata(dbg, f.path)
        log.debug("%s: returning td %s" % (dbg, tap))
        return tap
    except xapi.storage.api.volume.Volume_does_not_exist:
        pass

def _metadata_dir(path):
    return TD_PROC_METADATA_DIR + "/" + os.path.realpath(path)

def save_tapdisk_metadata(dbg, path, tap):
    """ Record the tapdisk metadata for this VDI in host-local storage """
    dirname = _metadata_dir(path)
    try:
        os.makedirs(dirname, mode=0755)
    except OSError as e:
        if e.errno != 17:  # 17 == EEXIST, which is harmless
            raise e
    with open(dirname + "/" + TD_PROC_METADATA_FILE, "w") as fd:
        pickle.dump(tap.__dict__, fd)

def load_tapdisk_metadata(dbg, path):
    """Recover the tapdisk metadata for this VDI from host-local
       storage."""
    dirname = _metadata_dir(path)
    log.debug("%s: load_tapdisk_metadata: trying '%s'" % (dbg, dirname))
    filename = dirname + "/" + TD_PROC_METADATA_FILE
    if not(os.path.exists(filename)):
        # XXX throw a better exception
        raise xapi.storage.api.volume.Volume_does_not_exist(dirname)
    with open(filename, "r") as fd:
        meta = pickle.load(fd)
        tap = Tapdisk(meta['minor'], meta['pid'], meta['f'])
        tap.secondary = meta['secondary']
        return tap

def forget_tapdisk_metadata(dbg, path):
    """Delete the tapdisk metadata for this VDI from host-local storage."""
    dirname = _metadata_dir(path)
    try:
        os.unlink(dirname + "/" + TD_PROC_METADATA_FILE)
    except:
        pass

