import http.server
import json
import os
import pathlib
import socketserver
import tempfile
import threading
import unittest

from anytls_stage_consumer import identity
from anytls_stage_unix_consumer import LIMIT, STAGES, StageConsumer, poll_unix

EPOCH = '0123456789abcdef'
SCOPE = json.dumps(['synthetic-container', 'synthetic-start', 'synthetic-netns'])
KEY = b'synthetic-key'
UTC = '2026-10-17T10:00:00Z'


def fixture(sequence=1, epoch=EPOCH):
    batch = {'schema': 1, 'kind': 'diagnostic_not_failure', 'evidence_epoch': epoch, 'sequence': sequence,
             'capture_started': UTC, 'capture_ended': UTC, 'time': UTC, 'export_age_ms': 0.1,
             'payload_limited': False, 'omitted_sessions': 0, 'omitted_slow_events': 0,
             'metric_stage_names': STAGES, 'admission_policy': 'bounded_rotation_on_auth_not_population_sample',
             'live_authenticated_sessions': 1, 'tracked_live_sessions': 1, 'untracked_live_sessions': 0,
             'evicted_sessions_total': 0, 'lost_completion_total': 0, 'lost_batch_total': 0,
             'lost_slow_event_total': 0, 'lost_terminal_record_total': 0,
             'sessions': [{'session': epoch+'-1', 'socket_cookie': '1234', 'status': 'active',
                           'inflight_known': True, 'captured_at': UTC, 'session_age_ms': 100,
                           'pending_writers': 1, 'tracked_waiters': 1, 'writer_wait_coverage_complete': True,
                           'waiter_overflow_total': 0,
                           'active': [{'stage': 'writer_wait', 'frame_class': 'fin', 'bytes': 7, 'age_ms': 100}],
                           'metrics': [{'count': 1, 'total_ms': 100, 'max_ms': 100, 'errors': 0, 'slow_count': 1} for _ in STAGES]}],
             'slow_events': [{'started_at': UTC, 'completed_at': UTC, 'session': epoch+'-1',
                              'stage': 'writer_wait', 'frame_class': 'fin', 'bytes': 7, 'duration_ms': 100, 'failed': False}]}
    return {'schema': 1, 'kind': 'anytls_stage_snapshot', 'availability': 'ready', 'evidence_epoch': epoch,
            'sequence': sequence, 'age_ms': 1000, 'failed_exports_total': 0, 'snapshot': batch}


def raw(value):
    return json.dumps(value).encode()


class ConsumerTests(unittest.TestCase):
    def test_privacy_and_exact_cookie_join(self):
        out = StageConsumer().consume(raw(fixture()), KEY, SCOPE)
        self.assertEqual(out['availability'], 'ready')
        self.assertEqual(out['fault_count_increment'], 0)
        row = out['observations']['sessions'][0]
        self.assertEqual(row['socket_id'], identity(KEY, json.dumps([SCOPE, 'cookie', '1234'])))
        self.assertEqual(row['active'][0]['stage'], 'writer_wait')
        self.assertTrue(out['metrics_are_cumulative'])
        self.assertFalse(out['population_sample_complete'])
        serialized = json.dumps(out)
        for forbidden in (EPOCH, 'socket_cookie', 'synthetic-container', '1234'):
            self.assertNotIn(forbidden, serialized)

    def test_duplicate_gap_regression_restart_and_epoch(self):
        s = StageConsumer()
        self.assertFalse(s.consume(raw(fixture(1)), KEY, SCOPE)['publication_contiguous'])
        self.assertTrue(s.consume(raw(fixture(2)), KEY, SCOPE)['publication_contiguous'])
        repeat = s.consume(raw(fixture(2)), KEY, SCOPE)
        self.assertTrue(repeat['duplicate']); self.assertIsNone(repeat['observations'])
        s.unavailable('transport_error')
        self.assertTrue(s.consume(raw(fixture(2)), KEY, SCOPE)['duplicate'])
        self.assertFalse(s.consume(raw(fixture(3)), KEY, SCOPE)['publication_contiguous'])
        gap = s.consume(raw(fixture(5)), KEY, SCOPE)
        self.assertEqual(gap['skipped_publications'], 1); self.assertFalse(gap['publication_contiguous'])
        self.assertEqual(s.consume(raw(fixture(4)), KEY, SCOPE)['availability'], 'sequence_regression')
        self.assertTrue(s.consume(raw(fixture(5)), KEY, SCOPE)['duplicate'])
        restored = StageConsumer(s.checkpoint())
        self.assertTrue(restored.consume(raw(fixture(5)), KEY, SCOPE)['duplicate'])
        self.assertFalse(restored.consume(raw(fixture(6)), KEY, SCOPE)['publication_contiguous'])
        other = restored.consume(raw(fixture(1, 'fedcba9876543210')), KEY, SCOPE)
        self.assertFalse(other['duplicate']); self.assertFalse(other['publication_contiguous'])

    def test_inactive_awaiting_stale_failure_and_truncation_are_unknown(self):
        s = StageConsumer()
        for status in ('inactive', 'awaiting_sample'):
            value = fixture();value.update(availability=status, sequence=None, age_ms=None, snapshot=None)
            out = s.consume(raw(value), KEY, SCOPE)
            self.assertEqual(out['availability'], status);self.assertIsNone(out['observations'])
        value = fixture();value['age_ms'] = 25001
        self.assertEqual(s.consume(raw(value), KEY, SCOPE)['availability'], 'stale')
        value = fixture();value['snapshot'].update(payload_limited=True, omitted_sessions=32, omitted_slow_events=32, sessions=None, slow_events=None)
        out = s.consume(raw(value), KEY, SCOPE)
        self.assertTrue(out['observations']['payload_limited'])
        self.assertEqual(out['observations']['omitted_sessions'], 32)
        self.assertEqual(out['fault_count_increment'], 0)
        value = fixture(2);value['failed_exports_total']=1
        self.assertFalse(s.consume(raw(value), KEY, SCOPE)['publication_contiguous'])

    def test_no_guess_when_cookie_or_scope_missing(self):
        value = fixture();value['snapshot']['sessions'][0]['socket_cookie']=None
        out = StageConsumer().consume(raw(value), KEY, SCOPE)
        self.assertIsNone(out['observations']['sessions'][0]['socket_id'])
        self.assertEqual(StageConsumer().consume(raw(value), KEY, '')['availability'], 'missing_scope')

    def test_rejects_unbounded_unknown_or_invalid_fields(self):
        cases=[]
        v=fixture();v['snapshot']['sessions'][0]['active'][0]['stage']='sensitive arbitrary text';cases.append(v)
        v=fixture();v['snapshot']['target']='sensitive.example';cases.append(v)
        v=fixture();v['snapshot']['sessions']*=65;cases.append(v)
        v=fixture();v['age_ms']=float('nan');cases.append(v)
        v=fixture();v['sequence']=True;cases.append(v)
        v=fixture();v['snapshot']['sessions'][0]['active']=None;cases.append(v)
        v=fixture();v['snapshot']['sessions'][0]['metrics'][0]['errors']=-1;cases.append(v)
        for v in cases:
            out=StageConsumer().consume(raw(v), KEY, SCOPE)
            self.assertIn(out['availability'], ('invalid_payload', 'body_too_large'))
            self.assertIsNone(out['observations']);self.assertEqual(out['fault_count_increment'], 0)
            self.assertNotIn('sensitive', json.dumps(out))
        self.assertEqual(StageConsumer().consume(b'x'*(LIMIT+1), KEY, SCOPE)['availability'], 'body_too_large')

    def test_real_unix_http_read_and_failure_are_bounded(self):
        response = raw(fixture())
        status = [200]
        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path != '/diagnostics/anytls-stage':
                    self.send_error(404);return
                body = response if status[0] == 200 else b''
                self.send_response(status[0]);self.send_header('Content-Length', str(len(body)));self.end_headers()
                if body:
                    self.wfile.write(body)
            def log_message(self, *args):
                pass
        with tempfile.TemporaryDirectory(prefix='stage-uds-') as directory:
            path = str(pathlib.Path(directory)/'api.sock')
            server = socketserver.UnixStreamServer(path, Handler)
            os.chmod(path, 0o600)
            thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': .01})
            thread.start()
            try:
                state=StageConsumer();out=poll_unix(path,KEY,SCOPE,state,.5)
                self.assertEqual(out['availability'],'ready')
                self.assertTrue(poll_unix(path,KEY,SCOPE,state,.5)['duplicate'])
                status[0]=404
                missing=poll_unix(path,KEY,SCOPE,state,.5)
                self.assertEqual(missing['availability'],'http_status')
                self.assertEqual(missing['http_status_code'],404)
                self.assertIsNone(missing['observations'])
            finally:
                server.shutdown();thread.join();server.server_close()
            self.assertEqual(poll_unix(path,KEY,SCOPE,state,.1)['availability'],'transport_error')


if __name__ == '__main__':
    unittest.main()
