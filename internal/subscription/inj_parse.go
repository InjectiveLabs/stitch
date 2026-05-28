package subscription

import (
	"bytes"
	"encoding/json"
)

// InjSubscribeParams mirrors the wire shape:
//
//	{"method":"subscribe","params":{"subscription_id":"X","filter":{...}}}
type InjSubscribeParams struct {
	SubscriptionID string          `json:"subscription_id"`
	Filter         json.RawMessage `json:"filter"`
}

// InjUnsubscribeParams mirrors:
//
//	{"method":"unsubscribe","params":{"subscription_id":"X"}}
type InjUnsubscribeParams struct {
	SubscriptionID string `json:"subscription_id"`
}

// ParseInjSubscribeParams pulls subscription_id+filter out of a JSON-RPC
// `subscribe` request's params object. Returns false if the shape is
// wrong (fields missing, params not an object, etc.).
func ParseInjSubscribeParams(raw json.RawMessage) (InjSubscribeParams, bool) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || raw[0] != '{' {
		return InjSubscribeParams{}, false
	}
	var p InjSubscribeParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return InjSubscribeParams{}, false
	}
	if p.SubscriptionID == "" {
		return InjSubscribeParams{}, false
	}
	return p, true
}

// ParseInjUnsubscribeParams pulls subscription_id out of a JSON-RPC
// `unsubscribe` request's params object.
func ParseInjUnsubscribeParams(raw json.RawMessage) (InjUnsubscribeParams, bool) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || raw[0] != '{' {
		return InjUnsubscribeParams{}, false
	}
	var p InjUnsubscribeParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return InjUnsubscribeParams{}, false
	}
	if p.SubscriptionID == "" {
		return InjUnsubscribeParams{}, false
	}
	return p, true
}

// InjNotification is a partial decode of an /injstream-ws notification.
// We extract the JSON-RPC id and the cursor (block_height); the original
// raw bytes are preserved for forwarding.
type InjNotification struct {
	ID     json.RawMessage
	Cursor Cursor
	Raw    []byte
}

// ParseInjNotification recognizes a /injstream-ws notification frame:
//
//	{"jsonrpc":"2.0","id":<id>,"result":{"block_height":N,...}}
//
// Returns false (no error) on responses that aren't notifications, e.g.
// the literal "success" reply to a subscribe call. The caller distinguishes
// based on the result shape.
func ParseInjNotification(raw []byte) (InjNotification, bool) {
	var env struct {
		ID     json.RawMessage `json:"id"`
		Result json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(raw), &env); err != nil {
		return InjNotification{}, false
	}
	res := bytes.TrimSpace(env.Result)
	if len(res) == 0 || res[0] != '{' {
		// Not an object result → not a notification (subscribe ack is a string).
		return InjNotification{}, false
	}
	var body struct {
		BlockHeight uint64 `json:"block_height"`
	}
	if err := json.Unmarshal(res, &body); err != nil {
		return InjNotification{}, false
	}
	return InjNotification{
		ID:     env.ID,
		Cursor: Cursor{Height: int64(body.BlockHeight)},
		Raw:    raw,
	}, true
}

// RewriteInjNotificationID returns raw with the top-level "id" replaced
// with the given id bytes. Used to swap an internal id on resume back to
// the client-facing id.
func RewriteInjNotificationID(raw []byte, id json.RawMessage) ([]byte, bool) {
	var env map[string]json.RawMessage
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, false
	}
	env["id"] = id
	out, err := json.Marshal(env)
	if err != nil {
		return nil, false
	}
	return out, true
}
