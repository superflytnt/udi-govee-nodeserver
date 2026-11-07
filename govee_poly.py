#!/usr/bin/env python3

"""
This is a NodeServer for Twinkly written by automationgeek (Jean-Francois Tremblay)
based on the NodeServer template for Polyglot v2 written in Python2/3 by Einstein.42 (James Milne) milne.james@gmail.com

PATCHED 2026-11-02: developer-API rate-limit redesign (fork of UniversalDevicesInc-PG3/udi-govee-nodeserver v3.0.0, GPL-3.0).
  - ONE shared long-lived Govee client on ONE background asyncio loop (was: a new
    blind Govee instance per poll per node and per command, each with default
    remaining=100 so per-instance throttling never saw the true budget).
  - Controller fetches all device states ONCE per shortPoll and distributes to
    nodes (was: every node fetched the full device list + ALL device states for
    itself = 72 calls/cycle for 8 nodes; now 8 calls/cycle).
  - Shared per-minute budget gate with a command reserve: poll calls run only
    while the API-reported per-minute remaining is above CMD_RESERVE, to retain headroom
    for commands. This does not guarantee success or reserve daily quota;
    the poll loop re-checks available budget before every call.
  - Commands use the warm device cache (1 API call per command; was 2) and report
    honest results: drivers are only set on success, failures raise a Notice
    (was: ST set optimistically even when the API call 429'd).
"""

import udi_interface
import hashlib
import asyncio
import warnings
import time
import json
import sys
import threading
from copy import deepcopy
from govee_api_laggat import Govee, GoveeAbstractLearningStorage, GoveeLearnedInfo

LOGGER = udi_interface.LOGGER
SERVERDATA = json.load(open('server.json'))
VERSION = SERVERDATA['credits'][0]['version']

# PATCH: per-minute command reserve. Poll calls never consume the last
# CMD_RESERVE calls reported in the current API minute window. This is only
# best-effort headroom, not a guarantee against daily limits or other clients.
# The library throttle is lowered to the same threshold.
CMD_RESERVE = 2

def percent_to_library(value):
    """eISY percent -> library 0..254; ceil preserves percent on 0..100 APIs."""
    percent = int(value)
    if not 0 <= percent <= 100:
        raise ValueError('Brightness must be between 0 and 100 percent')
    return (percent * 254 + 99) // 100

def library_to_percent(value):
    """Library 0..254 -> nearest whole percent; never publish over 100%."""
    raw = max(0, min(254, int(value)))
    return (raw * 100 + 127) // 254

def get_profile_info(logger):
    pvf = 'profile/version.txt'
    try:
        with open(pvf) as f:
            pv = f.read().replace('\n', '')
    except Exception as err:
        logger.error('get_profile_info: failed to read  file {0}: {1}'.format(pvf,err), exc_info=True)
        pv = 0
    f.close()
    return { 'version': pv }

class Controller(udi_interface.Node):

    def __init__(self, polyglot, primary, address, name):
        super(Controller, self).__init__(polyglot, primary, address, name)
        self.poly = polyglot
        self.name = 'Govee'
        self.initialized = False
        self.queryON = False
        self.api_key = ""
        self.hb = 0
        # Optional external heartbeat URL (e.g. UptimeRobot heartbeat). Pinged
        # on every short poll regardless of device state; empty disables.
        self.heartbeat_url = ""
        self._heartbeat_last = 0.0  # monotonic time of last ping attempt

        # PATCH: shared Govee client living on a single background loop.
        self._loop = None
        self._govee = None
        self._govee_ready = threading.Event()

        polyglot.subscribe(polyglot.START, self.start, address)
        polyglot.subscribe(polyglot.CUSTOMPARAMS, self.parameterHandler)
        polyglot.subscribe(polyglot.POLL, self.poll)

        polyglot.ready()
        polyglot.addNode(self)

    # PATCH: --- shared client plumbing -------------------------------------
    def _loop_thread(self):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def _ensure_govee(self):
        """Create the shared client once the api_key is known."""
        if self._govee is not None:
            return True
        if not self.api_key:
            return False
        if self._loop is None:
            t = threading.Thread(target=self._loop_thread, daemon=True)
            t.start()
            # wait for the loop to be running
            for _ in range(100):
                if self._loop is not None and self._loop.is_running():
                    break
                time.sleep(0.05)
        async def _create():
            g = await Govee.create(self.api_key)
            # Warm the device cache once at startup (1 call). The library's own
            # 100s scheduler keeps it refreshed after that.
            await g.get_devices()
            # Lower the library sleep threshold to our reserve; the poll gate
            # (below) enforces the reserve for polling, commands keep priority.
            try:
                g._api.rate_limit_on = CMD_RESERVE
            except Exception as ex:
                LOGGER.warning('could not set rate_limit_on: %s', ex)
            return g
        fut = asyncio.run_coroutine_threadsafe(_create(), self._loop)
        self._govee = fut.result(60)
        self._govee_ready.set()
        return True

    def run_on_govee(self, coro, timeout=90):
        """Run a coroutine on the shared loop, serialized with everything else."""
        if not self._ensure_govee():
            raise RuntimeError('Govee client not initialized (no api_key?)')
        fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return fut.result(timeout)

    def _budget_ok(self):
        """True when a poll call may fire without touching the command reserve."""
        api = self._govee._api
        if api.rate_limit_reset_seconds <= 0:
            return True  # window rolled over; next response refreshes headers
        return api.rate_limit_remaining > CMD_RESERVE

    async def _refresh_all_states(self):
        """One shared state sweep for all devices, budget-gated per call."""
        states = {}
        api = self._govee._api
        for device in list(self._govee.devices):
            if not self._budget_ok():
                LOGGER.info('poll deferred: API minute budget reserve reached; '
                            'remaining devices update next cycle')
                break
            _, err = await api._get_device_state(device)
            if err:
                LOGGER.warning('state error for %s: %s', device.device, err)
                continue
            states[device.device] = device
        return states
    # PATCH: --- end shared client plumbing ---------------------------------

    def parameterHandler(self, params):
        self.poly.Notices.clear()
        try:
            if 'heartbeat_url' in params:
                self.heartbeat_url = params['heartbeat_url'].strip()
            else:
                self.heartbeat_url = ""
            if self.heartbeat_url:
                LOGGER.info('Heartbeat URL configured - pinging on every short poll')

            if 'api_key' in params:
                self.api_key = params['api_key']
            else:
                self.api_key = ""

            if self.api_key == "" :
                LOGGER.error('Govee requires \'api_key\' parameters to be specified in custom configuration.')
                self.poly.Notices['cfg'] = 'Govee requires an API key.'

                return False
            else:
                self._ensure_govee()   # PATCH: shared client before discover
                self.discover()
                # PATCH: initial shared sweep so restart doesn't leave stale
                # states until the first scheduled shortPoll.
                try:
                    self._distribute(self.run_on_govee(self._refresh_all_states()))
                except Exception as ex:
                    LOGGER.error('initial refresh failed: %s', ex)

        except Exception as ex:
            LOGGER.error('Error starting Govee: {}'.format(ex))

    def start(self):
        LOGGER.info('Started Govee NodeServer version %s (patched rate-limit redesign)', str(VERSION))
        self.setDriver('ST', 0)
        self.poly.updateProfile()
        self.poly.setCustomParamsDoc()

    def poll(self, pollflag):
        if 'shortPoll' in pollflag:
            self.setDriver('ST', 1)
            self._ping_heartbeat()
            # PATCH: one shared sweep, then distribute. Was: each node ran its
            # own _query() = 9 API calls per node per cycle.
            try:
                states = self.run_on_govee(self._refresh_all_states())
            except Exception as ex:
                LOGGER.error('poll refresh failed: %s', ex)
                return
            self._distribute(states)
        else:
            self.heartbeat()

    def _ping_heartbeat(self):
        """Ping the configured external heartbeat URL, if any.

        Fires on every short poll whether devices are on, off, or the API
        sweep fails: the signal is plugin liveness, not device state.
        Failures are logged, never raised - a network hiccup must not
        break the poll loop.
        """
        if not self.heartbeat_url:
            return
        # Throttle attempts to at most one per 60s: monitoring endpoints
        # (e.g. UptimeRobot) rate-limit faster pings with HTTP 429, and the
        # monitor only needs a ping inside its own expected interval.
        import time
        now = time.monotonic()
        if now - self._heartbeat_last < 60:
            return
        self._heartbeat_last = now
        import urllib.request
        try:
            with urllib.request.urlopen(self.heartbeat_url, timeout=5) as resp:
                LOGGER.debug('heartbeat ping sent (HTTP %s)', resp.status)
        except Exception as ex:
            LOGGER.warning('heartbeat ping failed: %s', ex)

    def _distribute(self, states):
        for node in self.poly.nodes():
            if node.address != self.address and getattr(node, 'queryON', False):
                node.update_from_state(states.get(node.device_id))

    def query(self):
        for node in self.poly.nodes():
            node.reportDrivers()

    def heartbeat(self):
        LOGGER.debug('heartbeat: hb={}'.format(self.hb))
        if self.hb == 0:
            self.reportCmd("DON",2)
            self.hb = 1
        else:
            self.reportCmd("DOF",2)
            self.hb = 0

    def discover(self, *args, **kwargs):
        # PATCH: discover via the shared client (warm cache, 1 API call).
        try:
            devices, err = self.run_on_govee(self._govee.get_devices())
        except Exception as ex:
            LOGGER.error('discover failed: %s', ex)
            return
        for device in devices:
            strHashDevice = str(int(hashlib.md5(device.device.encode('utf8')).hexdigest(), 16) % (10 ** 8))
            self.poly.addNode(GoveeLight(self.poly, self.address, strHashDevice, strHashDevice, self.api_key, device.device ))

    def delete(self):
        LOGGER.info('Deleting Govee')
        # PATCH: shut down the shared loop cleanly.
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._loop.stop)

    id = 'controller'
    commands = {
        'QUERY': query,
        'DISCOVER': discover,
    }
    drivers = [{'driver': 'ST', 'value': 1, 'uom': 2}]

class GoveeLight(udi_interface.Node):

    def __init__(self, controller, primary, address, name, api_key, device_id):

        super(GoveeLight, self).__init__(controller, primary, address, name)
        self.queryON = True
        self.api_key = api_key
        self.device_id = device_id

        controller.subscribe(controller.START, self.start, address)

    def start(self):
        # PATCH: no per-node API storm at startup; states arrive via the
        # controller's shared poll sweep.
        pass

    # PATCH: honest commands - 1 API call each via the shared client's warm
    # cache, drivers set only on confirmed success.
    def _run_cmd(self, coro_factory, on_success):
        try:
            ctrl = self.poly.getNode('controller')
            success, err = ctrl.run_on_govee(coro_factory(ctrl._govee))
            if success:
                on_success()
            else:
                LOGGER.error('command failed for %s: %s', self.device_id, err)
                ctrl.poly.Notices['cmd'] = 'Govee command failed ({}): {}'.format(self.name, err)
        except Exception as ex:
            LOGGER.error('command error for %s: %s', self.device_id, ex)

    def setOn(self, command):
        self._run_cmd(lambda g: g.turn_on(self.device_id),
                      lambda: self.setDriver('ST', 100, True))

    def setOff(self, command):
        self._run_cmd(lambda g: g.turn_off(self.device_id),
                      lambda: self.setDriver('ST', 0, True))

    def setBrightness(self, command):
        try:
            raw = percent_to_library(command.get('value'))
        except (ValueError, TypeError) as ex:
            LOGGER.error('invalid brightness for %s: %s', self.device_id, ex)
            return
        # Report the library's applied/quantized value, not the input request.
        def applied():
            ctrl = self.poly.getNode('controller')
            device = next(d for d in ctrl._govee.devices if d.device == self.device_id)
            self.setDriver('GV1', library_to_percent(device.brightness))
        async def set_percent(g):
            device = next(d for d in g.devices if d.device == self.device_id)
            # This plugin uses Govee's legacy HTTP API, whose documented command
            # range is 0..100. Do not let the library guess 0..254 on first use:
            # a value below 100 would succeed at the wrong physical brightness.
            device.learned_set_brightness_max = 100
            return await g.set_brightness(self.device_id, raw)
        self._run_cmd(set_percent, applied)

    def setColor(self,command):
        try:
            query = command.get('query')
            color_r = int(query.get('R.uom100'))
            color_g = int(query.get('G.uom100'))
            color_b = int(query.get('B.uom100'))
            color = [color_r, color_g, color_b]
        except Exception as ex:
            LOGGER.error('setColor parse:', ex)
            return
        def _applied():
            self.setDriver('GV6', color_r)
            self.setDriver('GV7', color_g)
            self.setDriver('GV8', color_b)
        self._run_cmd(lambda g: g.set_color(self.device_id, color), _applied)

    def update(self):
        # PATCH: kept for interface compatibility; real refresh is the shared
        # sweep in Controller.poll -> update_from_state.
        pass

    def update_from_state(self, deviceState):
        # PATCH: apply a state fetched by the controller's shared sweep.
        try:
            if deviceState is None:
                return  # deferred by budget gate or errored; keep last values
            notice_key = 'offline_' + self.address
            if deviceState.online is not True:
                self.poly.Notices[notice_key] = '{} is offline or has unknown reachability; displayed values are last known.'.format(self.name)
                # Migrate only an old invalid 0..254 display value. This is a
                # unit correction, not a fresh observation of the offline light.
                old_brightness = self.getDriver('GV1')
                if old_brightness is not None and int(old_brightness) > 100:
                    self.setDriver('GV1', library_to_percent(old_brightness))
                return  # Do not turn stale powerState into a new On/Off report.
            self.poly.Notices.delete(notice_key)
            if deviceState.power_state:
                self.setDriver('ST', 100)
            else:
                self.setDriver('ST', 0)
            self.setDriver('GV1', library_to_percent(deviceState.brightness))
            self.setDriver('GV6', int(deviceState.color[0]))
            self.setDriver('GV7', int(deviceState.color[1]))
            self.setDriver('GV8', int(deviceState.color[2]))
        except Exception as ex:
            LOGGER.error('update_from_state:', ex)

    def query(self):
        self.reportDrivers()

    drivers = [{'driver': 'ST', 'value': 0, 'uom': 78},
               {'driver': 'GV1', 'value': 0, 'uom': 51},
               {'driver': 'GV6', 'value': 0, 'uom': 100},
               {'driver': 'GV7', 'value': 0, 'uom': 100},
               {'driver': 'GV8', 'value': 0, 'uom': 100}]

    id = 'GOVEE_LIGHT'
    commands = {
                    'DON': setOn,
                    'DOF': setOff,
                    'SET_BRI': setBrightness,
                    'SET_COLORID': setColor
                }

if __name__ == "__main__":
    try:
        polyglot = udi_interface.Interface([])
        polyglot.start()
        Controller(polyglot, 'controller', 'controller', 'GoveeNodeServer')
        polyglot.runForever()
    except (KeyboardInterrupt, SystemExit):
        sys.exit(0)
