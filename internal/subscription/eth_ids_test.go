package subscription

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"
)

type captureEthIO struct {
	upstream [][]byte
	client   [][]byte
}

func (io *captureEthIO) upstreamWrite(msg []byte) error {
	io.upstream = append(io.upstream, append([]byte(nil), msg...))
	return nil
}

func (io *captureEthIO) clientWrite(msg []byte) error {
	io.client = append(io.client, append([]byte(nil), msg...))
	return nil
}

func (io *captureEthIO) reply(id json.RawMessage, key string, value any) error {
	msg, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, key: value})
	if err != nil {
		return err
	}
	return io.clientWrite(msg)
}

func (io *captureEthIO) clientReplyResult(id json.RawMessage, result string) error {
	return io.reply(id, "result", result)
}

func (io *captureEthIO) clientReplyBool(id json.RawMessage, result bool) error {
	return io.reply(id, "result", result)
}

func (io *captureEthIO) clientReplyError(id json.RawMessage, code int, msg string) error {
	return io.reply(id, "error", map[string]any{"code": code, "message": msg})
}

func ethEnvelope(t *testing.T, msg []byte) map[string]json.RawMessage {
	t.Helper()
	var result map[string]json.RawMessage
	if err := json.Unmarshal(msg, &result); err != nil {
		t.Fatalf("invalid JSON %s: %v", msg, err)
	}
	return result
}

func numericEthID(t *testing.T, msg []byte) json.RawMessage {
	t.Helper()
	id := ethEnvelope(t, msg)["id"]
	if _, err := strconv.ParseUint(string(id), 10, 64); err != nil {
		t.Fatalf("upstream ID must be an unquoted integer, got %s", id)
	}
	return id
}

func ethAck(t *testing.T, a *ethAdapter, io *captureEthIO, id json.RawMessage, result string) {
	t.Helper()
	msg, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "result": result})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.HandleUpstreamFrame(io, msg); err != nil {
		t.Fatal(err)
	}
}

func TestEthSubscriptionNumericUpstreamIDsPreserveClientIDs(t *testing.T) {
	for _, clientID := range []string{`1`, `"subscription-request"`, `9007199254740993`, `null`} {
		t.Run(clientID, func(t *testing.T) {
			a, io := newEthAdapter(), &captureEthIO{}
			req := []byte(`{"jsonrpc":"2.0","id":` + clientID + `,"method":"eth_subscribe","params":["newHeads"]}`)
			if err := a.HandleClientFrame(io, req); err != nil {
				t.Fatal(err)
			}
			firstID := numericEthID(t, io.upstream[0])
			ethAck(t, a, io, firstID, "0xupstream")
			response := ethEnvelope(t, io.client[0])
			if string(response["id"]) != clientID || string(response["result"]) != `"0x0000000000000001"` {
				t.Fatalf("unexpected client subscribe response: %s", io.client[0])
			}
			// A real notification advances the cursor before reconnect/replay.
			notification := []byte(`{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xupstream","result":{"number":"0x64"}}}`)
			if err := a.HandleUpstreamFrame(io, notification); err != nil {
				t.Fatal(err)
			}
			if err := a.ReplaySubs(context.Background(), io); err != nil {
				t.Fatal(err)
			}
			replayID := numericEthID(t, io.upstream[1])
			if string(firstID) == string(replayID) {
				t.Fatal("replay reused the original request ID")
			}
			ethAck(t, a, io, replayID, "0xresumed")
			if len(io.client) != 2 {
				t.Fatalf("replay generated an extra client reply: %q", io.client)
			}
			unsubscribe := []byte(`{"jsonrpc":"2.0","id":` + clientID + `,"method":"eth_unsubscribe","params":["0x0000000000000001"]}`)
			if err := a.HandleClientFrame(io, unsubscribe); err != nil {
				t.Fatal(err)
			}
			unsubscribeID := numericEthID(t, io.upstream[2])
			response = ethEnvelope(t, io.client[2])
			if string(response["id"]) != clientID || string(response["result"]) != "true" {
				t.Fatalf("unexpected unsubscribe reply: %s", io.client[2])
			}
			if err := a.HandleUpstreamFrame(io, []byte(`{"jsonrpc":"2.0","id":`+string(unsubscribeID)+`,"result":true}`)); err != nil {
				t.Fatal(err)
			}
			if len(io.client) != 3 {
				t.Fatalf("internal unsubscribe ack leaked to client: %q", io.client)
			}
		})
	}
}

func TestEthOrdinaryCallCannotCollideWithPendingSubscribe(t *testing.T) {
	a, io := newEthAdapter(), &captureEthIO{}
	for _, req := range []string{
		`{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["newHeads"]}`,
		`{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}`,
	} {
		if err := a.HandleClientFrame(io, []byte(req)); err != nil {
			t.Fatal(err)
		}
	}
	subID := numericEthID(t, io.upstream[0])
	callID := numericEthID(t, io.upstream[1])
	if string(subID) == string(callID) {
		t.Fatal("ordinary call collided with pending subscribe")
	}
	// The ordinary reply arrives before the subscribe acknowledgement.
	ethAck(t, a, io, callID, "0x59f")
	ethAck(t, a, io, subID, "0xsubscription")
	for index, wantResult := range []string{`"0x59f"`, `"0x0000000000000001"`} {
		reply := ethEnvelope(t, io.client[index])
		if string(reply["id"]) != "1" || string(reply["result"]) != wantResult {
			t.Fatalf("reply %d was miscorrelated: %s", index, io.client[index])
		}
	}
}

func TestEthBatchIDsPreserveTypePrecisionAndResponseShape(t *testing.T) {
	a, io := newEthAdapter(), &captureEthIO{}
	clientIDs := []string{`1`, `"1"`, `9007199254740993`, `null`}
	batch := make([]json.RawMessage, 0, len(clientIDs))
	for _, id := range clientIDs {
		batch = append(batch, json.RawMessage(`{"jsonrpc":"2.0","id":`+id+`,"method":"eth_chainId","params":[]}`))
	}
	req, err := json.Marshal(batch)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.HandleClientFrame(io, req); err != nil {
		t.Fatal(err)
	}
	var upstream []json.RawMessage
	if err := json.Unmarshal(io.upstream[0], &upstream); err != nil {
		t.Fatal(err)
	}
	responses := make([]json.RawMessage, 0, len(upstream))
	for i := len(upstream) - 1; i >= 0; i-- {
		id := numericEthID(t, upstream[i])
		responses = append(responses, json.RawMessage(`{"jsonrpc":"2.0","id":`+string(id)+`,"result":"0x59f"}`))
	}
	response, err := json.Marshal(responses)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.HandleUpstreamFrame(io, response); err != nil {
		t.Fatal(err)
	}
	var restored []json.RawMessage
	if err := json.Unmarshal(io.client[0], &restored); err != nil {
		t.Fatalf("batch response shape was lost: %v", err)
	}
	for i, msg := range restored {
		id := ethEnvelope(t, msg)["id"]
		if string(id) != clientIDs[len(clientIDs)-1-i] {
			t.Fatalf("response ID lost type/precision: %s", id)
		}
	}
}

func TestEthReplayDiscardsAbandonedOrdinaryCallIDs(t *testing.T) {
	a, io := newEthAdapter(), &captureEthIO{}
	if err := a.HandleClientFrame(io, []byte(`{"jsonrpc":"2.0","id":"unfinished","method":"eth_chainId","params":[]}`)); err != nil {
		t.Fatal(err)
	}
	if len(a.rpcPending) != 1 {
		t.Fatal("ordinary request did not register correlation")
	}
	if err := a.ReplaySubs(context.Background(), io); err != nil {
		t.Fatal(err)
	}
	if len(a.rpcPending) != 0 {
		t.Fatal("dead upstream correlation entries survived reconnect")
	}
}
