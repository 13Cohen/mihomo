import json
import unittest
from anytls_stage_consumer import consume, classify, identity

class ConsumerTests(unittest.TestCase):
    def fixture(self, cookie='1234'):
        return {'schema': 1, 'kind': 'diagnostic_not_failure',
                'lost_batch_total': 0, 'lost_slow_event_total': 0,
                'untracked_live_sessions': 0,
                'sessions': [{'session': '0123456789abcdef-1',
                              'socket_cookie': cookie,
                              'inflight_known': True,
                              'writer_wait_coverage_complete': True}],
                'slow_events': []}

    def test_warning_is_diagnostic_and_scoped_cookie_matches(self):
        payload = '[AnyTLS EVIDENCE] ' + json.dumps(self.fixture())
        line = json.dumps({'log': 'time="synthetic" level=warning msg=' + json.dumps(payload) + '\n'})
        key, scope = b'fixture-key', json.dumps(['fixture-container', 'fixture-epoch', 'fixture-netns'])
        out = consume(line, key, scope)
        self.assertEqual(classify(line), 'diagnostic:anytls_stage')
        self.assertEqual(out['fault_count_increment'], 0)
        self.assertEqual(out['rows'][0]['socket_id'], identity(key, json.dumps([scope, 'cookie', '1234'])))
        self.assertNotEqual(out['rows'][0]['socket_id'], consume(line, key, scope+'changed')['rows'][0]['socket_id'])
        serialized = json.dumps(out)
        self.assertNotIn('socket_cookie', serialized)
        self.assertNotIn('0123456789abcdef-1', serialized)

    def test_missing_cookie_and_epoch_are_unavailable(self):
        for cookie, scope in [(None, 'scope'), ('0', 'scope'), ('bad-sensitive-field', 'scope'), ('1234', '')]:
            out = consume('[AnyTLS EVIDENCE] '+json.dumps(self.fixture(cookie)), b'key', scope)
            self.assertIsNone(out['rows'][0]['socket_id'])
            self.assertEqual(out['rows'][0]['association'], 'unavailable')

    def test_overflow_and_malformed_diagnostic_are_not_faults(self):
        data = self.fixture();data['untracked_live_sessions']=7;data['lost_batch_total']=3
        out = consume('[AnyTLS EVIDENCE] '+json.dumps(data), b'key', 'scope')
        self.assertEqual(out['untracked_live_sessions'],7)
        self.assertEqual(out['lost_batch_total'],3)
        self.assertEqual(classify('level=warning [AnyTLS EVIDENCE] bad-json'), 'diagnostic:anytls_stage_invalid')
        self.assertIsNone(classify('level=warning other event'))

if __name__ == '__main__':
    unittest.main()
