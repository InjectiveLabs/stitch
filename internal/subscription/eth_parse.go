package subscription

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
)

// EthNotification is a partial decode of an eth_subscription notification
// envelope. We only extract the fields the hub needs (subscription id and
// the cursor-relevant numbers); the full payload is preserved as raw
// bytes so the hub can forward it verbatim.
type EthNotification struct {
	SubscriptionID string
	Cursor         Cursor
	Raw            []byte
}

// ethNotificationEnvelope mirrors the wire shape:
//
//	{
//	  "jsonrpc":"2.0",
//	  "method":"eth_subscription",
//	  "params":{ "subscription":"0x...", "result": <header|log|...> }
//	}
type ethNotificationEnvelope struct {
	JSONRPC string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  struct {
		Subscription string          `json:"subscription"`
		Result       json.RawMessage `json:"result"`
	} `json:"params"`
}

// ParseEthNotification reads a JSON-RPC notification frame and pulls out
// the subscription id and a cursor derived from the result body. Kind
// drives which fields produce the cursor.
//
// Returns false (no error) if the frame is not an eth_subscription
// notification — that lets the caller distinguish notifications from
// regular JSON-RPC responses without raising errors on every frame.
func ParseEthNotification(raw []byte, kind Kind) (EthNotification, bool) {
	var env ethNotificationEnvelope
	if err := json.Unmarshal(bytes.TrimSpace(raw), &env); err != nil {
		return EthNotification{}, false
	}
	if env.Method != "eth_subscription" || env.Params.Subscription == "" {
		return EthNotification{}, false
	}
	cur := cursorFromResult(env.Params.Result, kind)
	return EthNotification{
		SubscriptionID: env.Params.Subscription,
		Cursor:         cur,
		Raw:            raw,
	}, true
}

// RewriteSubscriptionID returns raw with the subscription id field
// replaced. Used to swap the upstream-minted id for stitch's synthetic
// id before forwarding to the client.
func RewriteSubscriptionID(raw []byte, newID string) ([]byte, bool) {
	var env struct {
		JSONRPC string                 `json:"jsonrpc"`
		Method  string                 `json:"method"`
		Params  map[string]any         `json:"params"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, false
	}
	if env.Params == nil {
		return nil, false
	}
	env.Params["subscription"] = newID
	out, err := json.Marshal(env)
	if err != nil {
		return nil, false
	}
	return out, true
}

// cursorFromResult parses the result body for the cursor fields relevant
// to the kind. Unknown / unparseable bodies return the zero cursor; the
// caller (hub) treats that as "do not advance".
func cursorFromResult(result json.RawMessage, kind Kind) Cursor {
	switch kind {
	case KindEthNewHeads:
		var hdr struct {
			Number string `json:"number"`
		}
		if err := json.Unmarshal(result, &hdr); err != nil {
			return Cursor{}
		}
		h, ok := parseHexInt(hdr.Number)
		if !ok {
			return Cursor{}
		}
		return Cursor{Height: h}
	case KindEthLogs:
		var lg struct {
			BlockNumber      string `json:"blockNumber"`
			TransactionIndex string `json:"transactionIndex"`
			LogIndex         string `json:"logIndex"`
		}
		if err := json.Unmarshal(result, &lg); err != nil {
			return Cursor{}
		}
		bn, _ := parseHexInt(lg.BlockNumber)
		ti, _ := parseHexInt(lg.TransactionIndex)
		li, _ := parseHexInt(lg.LogIndex)
		return Cursor{Height: bn, TxIndex: ti, LogIndex: li}
	}
	return Cursor{}
}

func parseHexInt(s string) (int64, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, false
	}
	if strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X") {
		v, err := strconv.ParseInt(s[2:], 16, 64)
		return v, err == nil
	}
	v, err := strconv.ParseInt(s, 10, 64)
	return v, err == nil
}
