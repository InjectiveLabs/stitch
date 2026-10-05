package subscription

import (
	"encoding/json"
	"testing"
)

func TestEthSubscriptionBatchesAreRejectedBeforeForwarding(t *testing.T) {
	for _, tc := range []struct {
		name  string
		body  string
		ids   []string
		codes []int
	}{
		{"subscribe", `[{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["newHeads"]}]`, []string{"1"}, []int{-32000}},
		{"mixed IDs", `[{"jsonrpc":"2.0","id":9007199254740993,"method":"eth_chainId"},{"jsonrpc":"2.0","id":"1","method":"eth_subscribe","params":["newHeads"]},{"jsonrpc":"2.0","id":null,"method":"eth_chainId"},{"jsonrpc":"2.0","id":"1","method":"eth_chainId"}]`, []string{"9007199254740993", `"1"`, "null", `"1"`}, []int{-32000, -32000, -32000, -32000}},
		{"unsubscribe", `[{"jsonrpc":"2.0","id":"cancel","method":"eth_unsubscribe","params":["0x0000000000000001"]},{"jsonrpc":"2.0","id":2,"method":"eth_chainId"}]`, []string{`"cancel"`, "2"}, []int{-32000, -32000}},
		{"notifications only", `[{"jsonrpc":"2.0","method":"eth_subscribe","params":["newHeads"]},{"jsonrpc":"2.0","method":"eth_chainId"}]`, nil, nil},
		{"mixed notification", `[{"jsonrpc":"2.0","id":2,"method":"eth_chainId"},{"jsonrpc":"2.0","method":"eth_unsubscribe","params":["0x0000000000000001"]}]`, []string{"2"}, []int{-32000}},
		{"invalid members", `[1,null,{"foo":"bar"},{"jsonrpc":"2.0","id":{},"method":"eth_chainId"},{"jsonrpc":"2.0","id":3,"method":"eth_subscribe","params":["newHeads"]}]`, []string{"null", "null", "null", "null", "3"}, []int{-32600, -32600, -32600, -32600, -32000}},
		{"invalid notification and subscription", `[{"jsonrpc":"2.0","method":null},{"jsonrpc":"2.0","id":4,"method":null},{"jsonrpc":1,"id":9,"method":"eth_subscribe"},{"jsonrpc":"2.0","method":"eth_chainId","params":true}]`, []string{"null", "null", "null", "null"}, []int{-32600, -32600, -32600, -32600}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, io := newEthAdapter(), &captureEthIO{}
			// A rejected unsubscribe batch must leave this existing subscription live.
			if err := a.HandleClientFrame(io, []byte(`{"jsonrpc":"2.0","id":10,"method":"eth_subscribe","params":["newHeads"]}`)); err != nil {
				t.Fatal(err)
			}
			ethAck(t, a, io, numericEthID(t, io.upstream[0]), "0xupstream")
			if err := a.HandleClientFrame(io, []byte(`{"jsonrpc":"2.0","id":"pending","method":"eth_chainId"}`)); err != nil {
				t.Fatal(err)
			}
			beforeID := a.idSeq.Load()
			io.upstream, io.client = nil, nil
			if err := a.HandleClientFrame(io, []byte(tc.body)); err != nil {
				t.Fatal(err)
			}
			if len(io.upstream) != 0 {
				t.Fatalf("rejected batch reached upstream: %q", io.upstream)
			}
			if a.idSeq.Load() != beforeID || len(a.subs) != 1 || len(a.upToSyn) != 1 || len(a.pending) != 0 || len(a.rpcPending) != 1 {
				t.Fatal("rejected batch changed correlation or subscription state")
			}
			if len(tc.ids) == 0 {
				if len(io.client) != 0 {
					t.Fatalf("notification-only batch received a reply: %q", io.client)
				}
			} else {
				if len(io.client) != 1 {
					t.Fatalf("expected one batch reply, got %q", io.client)
				}
				var replies []struct {
					ID      json.RawMessage `json:"id"`
					JSONRPC string          `json:"jsonrpc"`
					Error   struct {
						Code    int    `json:"code"`
						Message string `json:"message"`
					} `json:"error"`
				}
				if err := json.Unmarshal(io.client[0], &replies); err != nil {
					t.Fatalf("reply is not an array: %s", io.client[0])
				}
				if len(replies) != len(tc.ids) {
					t.Fatalf("wrong number of replies: %s", io.client[0])
				}
				for i, reply := range replies {
					if string(reply.ID) != tc.ids[i] || reply.JSONRPC != "2.0" || reply.Error.Code != tc.codes[i] || reply.Error.Message == "" {
						t.Fatalf("incorrect reply %d: %s", i, io.client[0])
					}
				}
			}
			io.client = nil
			if err := a.HandleUpstreamFrame(io, []byte(`{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xupstream","result":{"number":"0x64"}}}`)); err != nil {
				t.Fatal(err)
			}
			if len(io.client) != 1 {
				t.Fatal("existing subscription stopped delivering after batch rejection")
			}
		})
	}
}

func TestEthNonSubscriptionBatchHandlingIsUnchanged(t *testing.T) {
	for _, body := range []string{`[]`, `[`, `[1]`, `[{"jsonrpc":"2.0","method":"eth_chainId"}]`} {
		t.Run(body, func(t *testing.T) {
			a, io := newEthAdapter(), &captureEthIO{}
			if err := a.HandleClientFrame(io, []byte(body)); err != nil {
				t.Fatal(err)
			}
			if len(io.client) != 0 || len(io.upstream) != 1 || string(io.upstream[0]) != body {
				t.Fatalf("ordinary/invalid batch handling changed: upstream=%q client=%q", io.upstream, io.client)
			}
		})
	}
}
