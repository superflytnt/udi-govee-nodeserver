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
    while the API-reported per-minute remaining is above CMD_RESERVE, so an eISY
    command always has calls left in the current minute. Commands preempt polls
    (single loop serializes; poll re-checks budget before every call).
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
# CMD_RESERVE calls of the current API minute window, so a command (1 call via
# the warm cache) can always execute. The library's own throttle is lowered to
# match so commands are not put to sleep while the reserve is intact.
CMD_RESERVE = 2

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
        bri = int(command.get('value'))
        self._run_cmd(lambda g: g.set_brightness(self.device_id, bri),
                      lambda: self.setDriver('GV1', bri))

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
            if deviceState.power_state:
                self.setDriver('ST', 100)
            else:
                self.setDriver('ST', 0)
            self.setDriver('GV1', int(deviceState.brightness))
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
