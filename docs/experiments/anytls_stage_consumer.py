#!/usr/bin/env python3
"""Prototype consumer contract; no connection to production collectors.

Raw input and cookies remain private. Only the existing scoped HMAC socket ID
is returned; a missing cookie is unavailable, never inferred from timing.
"""
import hashlib
import hmac
import json
import re

MARKER = '[AnyTLS EVIDENCE] '
TOKEN = re.compile(r'^[0-9a-f]{16}-[0-9a-f]{1,16}$')
COOKIE = re.compile(r'^[0-9a-f]{1,16}$')


def identity(key, value):
    return hmac.new(key, value.encode(), hashlib.sha256).hexdigest()[:24]


def decode(line):
    if len(line.encode()) > 128 * 1024:
        raise ValueError('bounded log input')
    # Docker JSON wraps logrus text; tests also accept the Event.Payload itself.
    if line.startswith('{'):
        outer = json.loads(line)
        if isinstance(outer, dict) and isinstance(outer.get('log'), str):
            line = outer['log']
    if ' msg="' in line:
        line, _ = json.JSONDecoder().raw_decode(line.split(' msg=', 1)[1])
    if MARKER not in line:
        return None
    raw = line.split(MARKER, 1)[1].strip()
    if len(raw.encode()) > 64 * 1024:
        raise ValueError('bounded stage batch')
    data = json.loads(raw)
    if not isinstance(data, dict) or data.get('schema') != 1 or data.get('kind') != 'diagnostic_not_failure':
        raise ValueError('unsupported stage schema')
    return data


def consume(line, key, scope):
    data = decode(line)
    if data is None:
        return None
    sessions, slow = data.get('sessions') or [], data.get('slow_events') or []
    if len(sessions) > 64 or len(slow) > 32:
        raise ValueError('bounded rows')
    rows = []
    for row in sessions:
        token, cookie = row.get('session'), row.get('socket_cookie')
        if not isinstance(token, str) or not TOKEN.fullmatch(token):
            raise ValueError('opaque session token')
        joined = None
        if scope and isinstance(cookie, str) and COOKIE.fullmatch(cookie) and int(cookie, 16):
            # This is precisely monitor.py parse_sockets' cookie alias input.
            joined = identity(key, json.dumps([scope, 'cookie', format(int(cookie, 16), 'x')]))
        rows.append({'session_id': identity(key, scope + '|' + token),
                     'socket_id': joined,
                     'association': 'cookie_candidate' if joined else 'unavailable',
                     'inflight_known': row.get('inflight_known') is True,
                     'waiter_coverage_complete': row.get('writer_wait_coverage_complete') is True})
    return {'classification': 'diagnostic:anytls_stage',
            'fault_count_increment': 0,
            'lost_batch_total': data.get('lost_batch_total'),
            'lost_slow_event_total': data.get('lost_slow_event_total'),
            'untracked_live_sessions': data.get('untracked_live_sessions'),
            'rows': rows}


def classify(line):
    # Put this BEFORE generic level=warning classification in a future consumer.
    # Malformed diagnostic input is a coverage problem, not a proxy failure.
    try:
        if decode(line) is not None:
            return 'diagnostic:anytls_stage'
    except (ValueError, TypeError, KeyError):
        if '[AnyTLS EVIDENCE]' in line:
            return 'diagnostic:anytls_stage_invalid'
        raise
    return None
