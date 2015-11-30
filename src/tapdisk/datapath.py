#!/usr/bin/env python

import urlparse
import os
import sys
import xapi
import xapi.storage.api.datapath
import xapi.storage.api.volume
from xapi.storage.datapath import tapdisk, image
from xapi.storage import log


class Implementation(xapi.storage.api.datapath.Datapath_skeleton):

    def activate(self, dbg, uri, domain):
        u = urlparse.urlparse(uri)
        # XXX need some datapath-specific errors below
        if not(os.path.exists(u.path)):
            raise xapi.storage.api.volume.Volume_does_not_exist(u.path)
        if u.scheme[:3] == "vhd":
            img = image.Vhd(u.path)
        elif u.scheme[:3] == "raw":
            img = image.Raw(u.path)
        else:
            raise

        # See whether we should open it O_DIRECT
        o_direct = self._get_uri_param(dbg, uri, 'o_direct', "true")
        o_direct = o_direct in ['true', 't', 'on', '1', 'yes']
        log.debug("o_direct = %s" % (o_direct))

        tap = tapdisk.load_tapdisk_metadata(dbg, uri)
        tap.open(dbg, img, o_direct)
        tapdisk.save_tapdisk_metadata(dbg, uri, tap)

    def _get_uri_param(self, dbg, uri, param_name, default=None):
        u = urlparse.urlparse(uri)
        q = urlparse.parse_qs(u.query)
        if param_name in q:
            return q[param_name][0]
        else:
            return default

    def attach(self, dbg, uri, domain):
        tap = tapdisk.create(dbg)
        tapdisk.save_tapdisk_metadata(dbg, uri, tap)
        return {
            'domain_uuid': '0',
            'implementation': ['Tapdisk3', tap.block_device()],
        }

    def close(self, dbg, uri):
        u = urlparse.urlparse(uri)
        # XXX need some datapath-specific errors below
        if not(os.path.exists(u.path)):
            raise xapi.storage.api.volume.Volume_does_not_exist(u.path)
        return None

    def detach(self, dbg, uri, domain):
        tap = tapdisk.load_tapdisk_metadata(dbg, uri)
        tap.destroy(dbg)
        tapdisk.forget_tapdisk_metadata(dbg, uri)

    def deactivate(self, dbg, uri, domain):
        tap = tapdisk.load_tapdisk_metadata(dbg, uri)
        tap.close(dbg)

    def open(self, dbg, uri, persistent):
        u = urlparse.urlparse(uri)
        # XXX need some datapath-specific errors below
        if not(os.path.exists(u.path)):
            raise xapi.storage.api.volume.Volume_does_not_exist(u.path)
        return None

if __name__ == "__main__":
    log.log_call_argv()
    cmd = xapi.storage.api.datapath.Datapath_commandline(Implementation())
    base = os.path.basename(sys.argv[0])
    if base == "Datapath.activate":
        cmd.activate()
    elif base == "Datapath.attach":
        cmd.attach()
    elif base == "Datapath.close":
        cmd.close()
    elif base == "Datapath.deactivate":
        cmd.deactivate()
    elif base == "Datapath.detach":
        cmd.detach()
    elif base == "Datapath.open":
        cmd.open()
    else:
        raise xapi.storage.api.datapath.Unimplemented(base)
