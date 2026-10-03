"""parse_endpoint() and format_endpoint() from Python: the C++ rules
(multiplexer/endpoint_test.cc has them all) reached through the extension,
a refusal as ValueError with the C++ reason, the default port optional,
and Endpoint the same object under multiplexer.threaded_client."""

import unittest

import multiplexer.threaded_client
from multiplexer.endpoints import Endpoint, format_endpoint, parse_endpoint


class Endpoints(unittest.TestCase):
    def test_parse(self):
        self.assertEqual(("127.0.0.1", 1980), parse_endpoint("127.0.0.1:1980"))
        self.assertEqual(("::1", 1980), parse_endpoint("[::1]:1980"))
        self.assertEqual(("", 1980), parse_endpoint(":1980"))
        self.assertEqual(("::", 1980), parse_endpoint("[::]", 1980))
        self.assertEqual(("host", 7), parse_endpoint("host:7", default_port=1980))

    def test_refusal_is_a_value_error_with_the_reason(self):
        for text, reason in (
            ("::1:1980", "'::1:1980': an IPv6 address goes in brackets, as in [::1]:1980"),
            ("[127.0.0.1]:1980", "'[127.0.0.1]:1980': brackets hold an IPv6 address"),
            ("host:-1", "'host:-1': the port is a number from 0 to 65535"),
            ("host", "'host': no port: host:port, or [address]:port for an IPv6 address"),
        ):
            with self.assertRaises(ValueError) as refused:
                parse_endpoint(text)
            self.assertEqual(reason, str(refused.exception))

    def test_format_and_back(self):
        self.assertEqual("[::1]:1980", format_endpoint(("::1", 1980)))
        self.assertEqual("127.0.0.1:0", format_endpoint(("127.0.0.1", 0)))
        for endpoint in [("::1", 0), ("10.0.0.1", 65535), ("mx.example.com", 1980)]:
            self.assertEqual(endpoint, parse_endpoint(format_endpoint(endpoint)))

    def test_endpoint_is_the_threaded_clients(self):
        self.assertIs(Endpoint, multiplexer.threaded_client.Endpoint)


if __name__ == "__main__":
    unittest.main()
