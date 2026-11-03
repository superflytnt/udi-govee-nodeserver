# Govee Polyglot V3 Node Server

## Why this fork exists

This fork builds on Jean-Francois Tremblay's (AutomationGeek) Govee node server and the govee-api-laggat library. It addresses problems found while using the plugin with PG3x and eISY:

- Each light could fetch the whole device list and every light's state. With eight lights, this could mean 72 requests in one polling cycle. This fork shares one client and one state sweep, checks the reported API budget before each poll request, and leaves some headroom for commands. It cannot guarantee that a command will avoid Govee's limits.
- The library uses a 0-254 brightness scale internally, while eISY and Govee's legacy HTTP API use percentages. This fork converts the readback and sets the API command range explicitly, including the first brightness command after startup.
- Offline devices can return stale power values. This fork keeps their last-known status and adds an offline notice rather than presenting stale data as a fresh observation. Command failures also produce a notice instead of an optimistic success status.

These changes were needed to reduce avoidable API-limit failures and make brightness and status useful in day-to-day control. On/off and percentage-brightness commands were tested through eISY on six reachable lights, with separate Govee API readbacks and state restoration. Scheduled polling was also observed after installation. Offline lights were not physically tested.

### Known limits

This still uses Govee's legacy developer API. App groups, sync boxes and other devices returned only by the newer API are not added by this repair. Offline status does not mean a device should be deleted. An intermittent eISY HTTP 404 was observed and later requests succeeded; its cause is not established and this fork does not claim to fix it.

The tested PG3x setup used a 900-second short poll and an 1800-second long poll. The older defaults in server.json and the installation notes below have not yet been revised for that setup.

![Build Status](https://travis-ci.org/therealmysteryman/udi-govee-nodeserver.svg?branch=master)

This Poly provides an interface between Govee and Polyglot v3 server. Provide basic control On/Off and Brightness of Govee WIFI Led Strip. Have been testing with H6104

## Installation

1 - The first thing would be to get the API key, this can be done by installing the 'Govee Home' app on your mobile and browse the user tab - About - Request API key. Usually you get your key within seconds by mail."

2 - Install Govee from Polyglot Store

3 - Create a custom variable called api_key and paste the api key received.

## Source

1. Using this Python Library to control the Govee - https://github.com/LaggAt/python-govee-api
2. Based on the Node Server Template - https://github.com/Einstein42/udi-poly-template-python
