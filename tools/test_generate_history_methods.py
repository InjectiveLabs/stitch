import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location("history_generator", Path(__file__).with_name("generate-history-methods.py"))
generator = importlib.util.module_from_spec(spec)
spec.loader.exec_module(generator)

class SchemaSelectionTests(unittest.TestCase):
    def test_unary_query_only_not_streams_or_writes(self):
        proto = '''
        syntax = "proto3";
        package cosmos.review.v1;
        service Query {
          rpc Balance(Request) returns (Response) {}
          rpc Watch(Request) returns (stream Response) {}
          rpc Upload(stream Request) returns (Response) {}
          rpc Chat(stream Request) returns (stream Response) {}
        }
        service Msg { rpc Send(Request) returns (Response) {} }
        service Stream { rpc Watch(Request) returns (Response) {} }
        '''
        self.assertEqual(generator.parse_methods(proto), ["/cosmos.review.v1.Query/Balance"])

    def test_comments_and_option_strings_cannot_create_methods(self):
        proto = '''package cosmos.review.v1;
        // service Query { rpc Fake(A) returns (B) {} }
        /* service Query { rpc AlsoFake(A) returns (B) {} } */
        service Query {
          option description = "} rpc Sneaky(A) returns (B) {";
          rpc Balance(A) returns (B) { option description = "// /* { }"; }
        }
        '''
        self.assertEqual(generator.parse_methods(proto), ["/cosmos.review.v1.Query/Balance"])

    def test_service_reads_require_exact_inventory(self):
        proto = '''package cosmos.tx.v1beta1;
        service Service {
          rpc BroadcastTx(A) returns (B) {}
          rpc Simulate(A) returns (B) {}
          rpc GetBlockWithTxs(A) returns (B) {}
        }'''
        self.assertEqual(generator.parse_methods(proto), ["/cosmos.tx.v1beta1.Service/GetBlockWithTxs"])

    def test_even_explicit_service_read_must_remain_unary(self):
        proto = '''package cosmos.tx.v1beta1;
        service Service { rpc GetBlockWithTxs(A) returns (stream B) {} }'''
        self.assertEqual(generator.parse_methods(proto), [])

if __name__ == "__main__":
    unittest.main()
