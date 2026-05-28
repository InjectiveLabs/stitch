package subscription

import (
	"strings"
	"testing"
)

func TestParseEthNotificationNewHeads(t *testing.T) {
	raw := []byte(`{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xabc","result":{"number":"0x10","hash":"0xdead"}}}`)
	n, ok := ParseEthNotification(raw, KindEthNewHeads)
	if !ok {
		t.Fatal("expected notification")
	}
	if n.SubscriptionID != "0xabc" {
		t.Errorf("sub id: %s", n.SubscriptionID)
	}
	if n.Cursor.Height != 0x10 {
		t.Errorf("cursor: %+v", n.Cursor)
	}
}

func TestParseEthNotificationLogs(t *testing.T) {
	raw := []byte(`{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xabc","result":{"blockNumber":"0x10","transactionIndex":"0x2","logIndex":"0x3"}}}`)
	n, ok := ParseEthNotification(raw, KindEthLogs)
	if !ok {
		t.Fatal("expected notification")
	}
	if n.Cursor != (Cursor{Height: 16, TxIndex: 2, LogIndex: 3}) {
		t.Errorf("cursor: %+v", n.Cursor)
	}
}

func TestParseEthNotificationIgnoresResponses(t *testing.T) {
	// Regular JSON-RPC response — not a notification.
	raw := []byte(`{"jsonrpc":"2.0","id":1,"result":"0xabc"}`)
	if _, ok := ParseEthNotification(raw, KindEthNewHeads); ok {
		t.Error("response must not parse as notification")
	}
}

func TestParseEthNotificationGarbage(t *testing.T) {
	if _, ok := ParseEthNotification([]byte(`not json`), KindEthNewHeads); ok {
		t.Error("garbage must not parse")
	}
}

func TestRewriteSubscriptionID(t *testing.T) {
	raw := []byte(`{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xabc","result":{"number":"0x10"}}}`)
	out, ok := RewriteSubscriptionID(raw, "0xdef")
	if !ok {
		t.Fatal("rewrite failed")
	}
	if !strings.Contains(string(out), `"subscription":"0xdef"`) {
		t.Errorf("rewrite produced: %s", out)
	}
	if strings.Contains(string(out), "0xabc") {
		t.Errorf("old id leaked: %s", out)
	}
}

func TestRewriteSubscriptionIDIgnoresMalformed(t *testing.T) {
	if _, ok := RewriteSubscriptionID([]byte(`not json`), "0xdef"); ok {
		t.Error("garbage must not rewrite")
	}
}
