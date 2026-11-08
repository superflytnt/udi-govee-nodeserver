# Govee Polyglot V3 Node Server

## Why this fork exists

This fork builds on Jean-Francois Tremblay's (AutomationGeek) Govee node server and the govee-api-laggat library. It addresses problems found while using the plugin with PG3x and eISY:

- Each light could fetch the whole device list and every light's state. With eight lights, this could mean 72 requests in one polling cycle. This fork shares one client and one state sweep, checks the reported API budget before each poll request, and leaves some headroom for commands. It cannot guarantee that a command will avoid Govee's limits.
- The library uses a 0-254 brightness scale internally, while eISY and Govee's legacy HTTP API use percentages. This fork converts the readback and sets the API command range explicitly, including the first brightness command after startup.
- Offline devices can return stale power values. This fork keeps their last-known status and adds an offline notice rather than presenting stale data as a fresh observation. Command failures also produce a notice instead of an optimistic success status.

### Version history

- **3.1.1** (2026-11-07): Heartbeat pings throttled to at most one per 60 seconds, to reduce rapid-ping HTTP 429 responses; monitor-specific limits still apply.
- **3.1.0** (2026-11-07): External heartbeat - optional `heartbeat_url` custom parameter. The plugin pings it on every short poll, regardless of device state or whether the API sweep succeeds, so an external cron monitor (e.g. UptimeRobot heartbeat) alerts when the plugin itself freezes. Empty (default) disables the ping.
- **3.0.1-patched** (2026-11-02): Rate-limit redesign described above: one shared client and state sweep, budget headroom for commands, honest command results.

These changes were needed to reduce avoidable API-limit failures and make brightness and status useful in day-to-day control. On/off and percentage-brightness commands were tested through eISY on six reachable lights, with separate Govee API readbacks and state restoration. Scheduled polling was also observed after installation. Offline lights were not physically tested.

### Known limits

This still uses Govee's legacy developer API. App groups, sync boxes and other devices returned only by the newer API are not added by this repair. Offline status does not mean a device should be deleted. An intermittent eISY HTTP 404 was observed and later requests succeeded; its cause is not established and this fork does not claim to fix it.

The published polling defaults in server.json are a 90-second short poll and a 600-second long poll. The setup this fork was tested with uses 900/1800 seconds. Adjust shortPoll/longPoll to your setup, and match any heartbeat monitor's expected interval to your actual poll cadence.

This plugin provides on/off, percentage-brightness control, status and reachability notices for compatible devices returned by Govee's legacy developer API. It does not add RGB color or color-temperature commands.

## Installation and configuration

### Local installation for development/testing

Public-store availability of this version has not been confirmed. Cloning the repository alone does not register or install a plugin in PG3(x).

Follow [UDI's node server publishing guide](https://wiki.universal-devices.com/Publishing_a_node_server_to_the_node_server_store) to create a **Local** store entry. Use this repository's `server.json` for the plugin fields and add a purchase option with the current version and these installation settings:

- Installation URL: `https://github.com/superflytnt/udi-govee-nodeserver`
- Branch: `main`
- Executable: `govee_poly.py`
- Install script: `install.sh`

Select the Local store, then install the entry through PG3(x). A Local entry is private to that PG3(x) instance. See [UDI's packaging guide](https://wiki.universal-devices.com/Packaging_a_node_server_for_distribution) for deployment details. Publishing access and any account requirements are handled by UDI.

1. Request a developer API key in the Govee Home app under your profile's About section, if the option is available for your account. Keep this key private.
2. Add the `api_key` custom parameter in PG3(x).
3. Optionally add `heartbeat_url` with a heartbeat/cron-monitor URL. Pings are attempted on short polls, at most once per 60 seconds, even when devices are off or the API sweep fails. Match the monitor's expected interval to your actual short-poll cadence. Leave it empty to disable.

## License

This fork retains the GNU GPL v3 license in [LICENSE](LICENSE). Original author and library credits are retained below and in `server.json`.

## Source

1. Using this Python Library to control the Govee - https://github.com/LaggAt/python-govee-api
2. Based on the Node Server Template - https://github.com/Einstein42/udi-poly-template-python
