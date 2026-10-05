package subscription

import (
	"bytes"
	"encoding/json"

	"github.com/InjectiveLabs/stitch/internal/cache"
)

// rejectEthSubscriptionBatch preflights the whole batch before any ID allocation
// or upstream write. Batch subscription acknowledgments cannot use the ordinary
// RPC correlation path: their IDs must belong to a tracked, resumable subscription.
// Reject mixed batches atomically so ordinary members cannot execute silently.
func rejectEthSubscriptionBatch(io sessionIO, msg []byte) (bool, error) {
	trimmed := bytes.TrimSpace(msg)
	if len(trimmed) == 0 || trimmed[0] != '[' {
		return false, nil
	}
	var batch []json.RawMessage
	if err := json.Unmarshal(trimmed, &batch); err != nil {
		return false, nil // Preserve upstream handling of malformed JSON.
	}
	members := make([]map[string]json.RawMessage, len(batch))
	containsSubscription := false
	for i, raw := range batch {
		_ = json.Unmarshal(raw, &members[i])
		var method string
		_ = json.Unmarshal(members[i]["method"], &method)
		if method == "eth_subscribe" || method == "eth_unsubscribe" {
			containsSubscription = true
		}
	}
	if !containsSubscription {
		return false, nil
	}

	responses := make([]map[string]any, 0, len(batch))
	for _, member := range members {
		var method, version string
		methodJSON := bytes.TrimSpace(member["method"])
		valid := len(methodJSON) > 0 && methodJSON[0] == '"' &&
			json.Unmarshal(methodJSON, &method) == nil &&
			json.Unmarshal(member["jsonrpc"], &version) == nil && version == "2.0"
		if params, exists := member["params"]; exists {
			params = bytes.TrimSpace(params)
			valid = valid && len(params) > 0 && (params[0] == '[' || params[0] == '{')
		}
		id, hasID := member["id"]
		valid = valid && (!hasID || cache.IsJSONRPCID(id))
		if valid && !hasID {
			continue // Notifications never receive a response, even on rejection.
		}
		code := -32000
		message := "batch contains subscription methods; send requests individually"
		if !valid {
			id, code, message = nil, -32600, "invalid request"
		}
		responses = append(responses, map[string]any{
			"jsonrpc": "2.0",
			"id":      id,
			"error":   map[string]any{"code": code, "message": message},
		})
	}
	if len(responses) == 0 {
		return true, nil
	}
	out, err := json.Marshal(responses)
	if err != nil {
		return true, err
	}
	return true, io.clientWrite(out)
}
