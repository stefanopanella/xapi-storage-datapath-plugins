#!/usr/bin/env python

import urlparse
import os
import sys
import xapi
import xapi.storage.api.datapath
import xapi.storage.api.volume
from xapi.storage.datapath import tapdisk, image
from xapi.storage import log
from xapi.storage.common import call
import time

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

        tap = tapdisk.load_tapdisk_metadata(dbg, u.path)
        tap.open(dbg, img, o_direct)
        tapdisk.save_tapdisk_metadata(dbg, u.path, tap)

    def _get_uri_param(self, dbg, uri, param_name, default=None):
        u = urlparse.urlparse(uri)
        q = urlparse.parse_qs(u.query)
        if param_name in q:
            return q[param_name][0]
        else:
            return default

    def attach(self, dbg, uri, domain):
        # FIXME: add lvm activation code
        u = urlparse.urlparse(uri)

        (vgname, lvname, scsid) = self._getVgLvScsid(dbg, u.path)
        log.debug("%s Vg=%s Lv=%s Scsid%s" % (dbg, vgname, lvname, scsid))
        vg = self._vgOpen(dbg, vgname, "r", scsid)
        lv = vg.lvFromName(lvname)
        lv.activate()
        vg.close()
        cmd = ["/usr/bin/vhd-util", "query", "-n", u.path, "-P"]
        output = call(dbg, cmd)
        log.debug("%s output=%s" % (dbg, output))
        output = output[:-1]
        if output[-6:] == "parent":
            log.debug("No Parent")
        else:
            output = output.replace("--", "-")
            log.debug("%s" % output[-36:])
            activation_file = "/var/run/nonpersistent/" + vgname + "/" + output[-36:]
            if (not os.path.exists(activation_file)):
                vg = self._vgOpen(dbg, vgname, "r", scsid)
                lv = vg.lvFromName(output[-36:])
                log.debug("Activating %s" % lv.getName())
                lv.activate()
                vg.close()
                open(activation_file, 'a').close()

        tap = tapdisk.create(dbg)
        tapdisk.save_tapdisk_metadata(dbg, u.path, tap)
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
        u = urlparse.urlparse(uri)
        tap = tapdisk.load_tapdisk_metadata(dbg, u.path)
        tap.destroy(dbg)
        tapdisk.forget_tapdisk_metadata(dbg, u.path)

        #(vgname, lvname, scsid) = self._getVgLvScsid(dbg, u.path)
        #log.debug("%s Vg=%s Lv=%s Scsid%s" % (dbg, vgname, lvname, scsid))
        #vg = self._vgOpen(dbg, vgname, "w", scsid)
        #lv = vg.lvFromName(lvname)
        #lv.deactivate()
        #cmd = ["/usr/bin/vhd-util", "query", "-n", u.path, "-P"]
        #output = call(dbg, cmd)
        #log.debug("%s output=%s" % (dbg, output))

    def deactivate(self, dbg, uri, domain):
        u = urlparse.urlparse(uri)
        tap = tapdisk.load_tapdisk_metadata(dbg, u.path)
        tap.close(dbg)

    def open(self, dbg, uri, persistent):
        u = urlparse.urlparse(uri)
        # XXX need some datapath-specific errors below
        if not(os.path.exists(u.path)):
            raise xapi.storage.api.volume.Volume_does_not_exist(u.path)
        return None

    def _getVgLvScsid(self, dbg, path):
        log.debug("%s path=%s" % (dbg, path))
        parts = path.split("/")
        vgname = parts[2]
        lvname = parts[3]
        with open("/var/run/nonpersistent/" + vgname + "/scsid", "r") as text_file:
            scsid = text_file.read()
        return (vgname, lvname, scsid)
   
    def _vgOpen(self, dbg, vgname, rw, scsid):
        log.debug("vgOpen")
        os.environ['LVM_DEVICE'] = "/dev/disk/by-scsid/" + scsid
        import lvm
        vg = lvm.vgOpen(vgname, rw)
        return vg

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
