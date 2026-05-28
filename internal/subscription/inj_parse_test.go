package subscription

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestParseInjSubscribeParamsHappy(t *testing.T) {
	p, ok := ParseInjSubscribeParams(json.RawMessage(`{"subscription_id":"abc","filter":{"foo":1}}`))
	if !ok {
		t.Fatal("expected ok")
	}
	if p.SubscriptionID != "abc" {
		t.Errorf("subscription_id: %q", p.SubscriptionID)
	}
	if !strings.Contains(string(p.Filter), `"foo":1`) {
		t.Errorf("filter: %s", p.Filter)
	}
}

func TestParseInjSubscribeParamsRejectsMissingID(t *testing.T) {
	if _, ok := ParseInjSubscribeParams(json.RawMessage(`{"filter":{}}`)); ok {
		t.Error("missing subscription_id should fail")
	}
	if _, ok := ParseInjSubscribeParams(json.RawMessage(`{"subscription_id":""}`)); ok {
		t.Error("empty subscription_id should fail")
	}
	if _, ok := ParseInjSubscribeParams(json.RawMessage(`null`)); ok {
		t.Error("null params should fail")
	}
}

func TestParseInjUnsubscribeParams(t *testing.T) {
	p, ok := ParseInjUnsubscribeParams(json.RawMessage(`{"subscription_id":"abc"}`))
	if !ok || p.SubscriptionID != "abc" {
		t.Errorf("got %+v ok=%v", p, ok)
	}
}

func TestParseInjNotification(t *testing.T) {
	raw := []byte(`{"jsonrpc":"2.0","id":42,"result":{"block_height":123,"block_time":456}}`)
	n, ok := ParseInjNotification(raw)
	if !ok {
		t.Fatal("expected notification")
	}
	if n.Cursor.Height != 123 {
		t.Errorf("cursor: %+v", n.Cursor)
	}
	if string(n.ID) != "42" {
		t.Errorf("id: %s", n.ID)
	}
}

func TestParseInjNotificationIgnoresSubscribeAck(t *testing.T) {
	// {"id":1,"result":"success"} is the subscribe ack, NOT a notification.
	raw := []byte(`{"jsonrpc":"2.0","id":1,"result":"success"}`)
	if _, ok := ParseInjNotification(raw); ok {
		t.Error("subscribe ack must not parse as notification")
	}
}

func TestParseInjNotificationGarbage(t *testing.T) {
	if _, ok := ParseInjNotification([]byte(`not json`)); ok {
		t.Error("garbage must not parse")
	}
}

func TestRewriteInjNotificationID(t *testing.T) {
	raw := []byte(`{"jsonrpc":"2.0","id":99,"result":{"block_height":1}}`)
	out, ok := RewriteInjNotificationID(raw, json.RawMessage(`42`))
	if !ok {
		t.Fatal("rewrite failed")
	}
	var env struct {
		ID json.RawMessage `json:"id"`
	}
	if err := json.Unmarshal(out, &env); err != nil {
		t.Fatal(err)
	}
	if string(env.ID) != "42" {
		t.Errorf("id rewritten to %s", env.ID)
	}
}
