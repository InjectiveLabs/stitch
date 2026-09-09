package cosmos_grpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/metadata"

	"github.com/InjectiveLabs/stitch/internal/backend"
	"github.com/InjectiveLabs/stitch/internal/types"
)

// Activation blocks can carry large result/event payloads. Keep their streaming
// read inside the overall discovery deadline while allowing more time than a
// small unary Params probe.
const earliestCometAttemptTimeout = 10 * time.Second

type earliestProofOp struct {
	Type string `json:"type"`
	Data string `json:"data"`
}

func validEarliestProof(ops []earliestProofOp) bool {
	if len(ops) == 0 {
		return false
	}
	for _, op := range ops {
		if op.Type == "" || op.Data == "" {
			return false
		}
	}
	return true
}

func validEarliestCapability(capability string) bool {
	switch capability {
	case "state", "block", "execution", "trace", "proof", "storage", "range":
		return true
	default:
		return false
	}
}

// Capability probes check the same concrete snapshot as the gateway's later
// operation. They test retained data, not whether an arbitrary execution or
// account lookup returns a nonzero value.
func (d *Director) earliestCapability(ctx context.Context, b *backend.Backend, md metadata.MD, capability string, height int64) (bool, error) {
	if capability == "state" {
		return true, nil
	}
	if capability == "trace" {
		if height <= 1 {
			return false, nil
		}
		parent := d.earliestInvoke(ctx, b, earliestParamsMethod, nil, md, height-1)
		if parent.err != nil {
			if missingEarliestHistory(parent.err) {
				return false, nil
			}
			return false, parent.err
		}
		if _, err := exactResponseHeight(parent.header, height-1); err != nil {
			return false, err
		}
		if initialized, err := initializedEVMParams(parent.payload); !initialized || err != nil {
			return false, err
		}
	}
	if b.Endpoint(types.ProtoRPC) == "" {
		return false, nil
	}
	h := strconv.FormatInt(height, 10)
	if capability != "storage" {
		raw, available, err := d.earliestComet(ctx, b, md, "block", map[string]any{"height": h})
		if !available || err != nil {
			return false, err
		}
		var block struct {
			Block *struct {
				Header struct {
					Height string `json:"height"`
				} `json:"header"`
			} `json:"block"`
		}
		if json.Unmarshal(raw, &block) != nil || block.Block == nil || block.Block.Header.Height != h {
			return false, errors.New("comet block response did not match requested height")
		}
	}
	if capability == "block" || capability == "range" || capability == "trace" {
		raw, available, err := d.earliestComet(ctx, b, md, "block_results", map[string]any{"height": h})
		if !available || err != nil {
			return false, err
		}
		var result struct {
			Height string `json:"height"`
		}
		if json.Unmarshal(raw, &result) != nil || result.Height != h {
			return false, errors.New("comet block results did not match requested height")
		}
	}
	if capability == "proof" {
		if height <= 2 {
			return false, nil
		}
		// evm Params key is 0x03. An absent zero-address auth key still has
		// a non-membership proof, which is a valid capability witness.
		for _, probe := range []struct{ path, key string }{
			{"/store/evm/key", "03"},
			{"/store/acc/key", "01" + strings.Repeat("00", 20)},
		} {
			if ok, err := d.earliestABCI(ctx, b, md, probe.path, probe.key, height, true); !ok || err != nil {
				return false, err
			}
		}
	}
	if capability == "storage" {
		return d.earliestABCI(ctx, b, md, "/store/evm/subspace", "02"+strings.Repeat("00", 20), height, false)
	}
	return true, nil
}

func (d *Director) earliestABCI(ctx context.Context, b *backend.Backend, md metadata.MD, path, key string, height int64, prove bool) (bool, error) {
	raw, available, err := d.earliestComet(ctx, b, md, "abci_query", map[string]any{
		// Comet's JSON HexBytes codec expects bare hexadecimal, unlike EVM
		// JSON-RPC's 0x-prefixed data representation.
		"path": path, "data": key, "height": strconv.FormatInt(height, 10), "prove": prove,
	})
	if !available || err != nil {
		return false, err
	}
	var result struct {
		Response *struct {
			Code     uint32 `json:"code"`
			Log      string `json:"log"`
			Height   string `json:"height"`
			ProofOps *struct {
				Ops []earliestProofOp `json:"ops"`
			} `json:"proofOps"`
			ProofOpsSnake *struct {
				Ops []earliestProofOp `json:"ops"`
			} `json:"proof_ops"`
		} `json:"response"`
	}
	if json.Unmarshal(raw, &result) != nil || result.Response == nil {
		return false, errors.New("invalid ABCI capability response")
	}
	response := result.Response
	if response.Code != 0 {
		if missingCometHistory(response.Log) || missingEarliestHistory(errors.New(response.Log)) {
			return false, nil
		}
		return false, fmt.Errorf("ABCI capability query failed: %s", response.Log)
	}
	if response.Height != strconv.FormatInt(height, 10) {
		return false, errors.New("ABCI response did not match requested height")
	}
	if prove && (response.ProofOps == nil || !validEarliestProof(response.ProofOps.Ops)) && (response.ProofOpsSnake == nil || !validEarliestProof(response.ProofOpsSnake.Ops)) {
		return false, nil
	}
	return true, nil
}

func (d *Director) earliestComet(ctx context.Context, b *backend.Backend, md metadata.MD, method string, params map[string]any) (result json.RawMessage, available bool, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("comet %s at height %v: %w", method, params["height"], err)
		}
	}()
	select {
	case d.earliestSlots <- struct{}{}:
		defer func() { <-d.earliestSlots }()
	case <-ctx.Done():
		return nil, false, ctx.Err()
	}
	if !d.circuit.Acquire(b.Name, types.ProtoRPC) {
		return nil, false, errors.New("comet backend circuit is open")
	}
	callCtx, cancel := context.WithTimeout(ctx, earliestCometAttemptTimeout)
	defer cancel()
	// Resolve this admission using only reachability, leaving valid domain
	// errors and absent historical capability neutral to circuit statistics.
	transportFailed := false
	succeeded := false
	defer func() {
		if succeeded {
			d.circuit.Record(b.Name, types.ProtoRPC, true)
		} else if transportFailed && ctx.Err() == nil {
			d.circuit.Record(b.Name, types.ProtoRPC, false)
		} else {
			d.circuit.Release(b.Name, types.ProtoRPC)
		}
	}()
	body, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	req, err := http.NewRequestWithContext(callCtx, http.MethodPost, b.Endpoint(types.ProtoRPC), bytes.NewReader(body))
	if err != nil {
		return nil, false, err
	}
	req.Header.Set("Content-Type", "application/json")
	if auth := md.Get("authorization"); len(auth) == 1 {
		req.Header.Set("Authorization", auth[0])
	}
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		transportFailed = true
		return nil, false, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		transportFailed = true
		return nil, false, fmt.Errorf("comet HTTP status %d", response.StatusCode)
	}
	payload, err := projectCometResponse(response.Body)
	if err != nil {
		transportFailed = true
		return nil, false, err
	}
	var envelope struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Code    int             `json:"code"`
			Message string          `json:"message"`
			Data    json.RawMessage `json:"data"`
		} `json:"error"`
	}
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return nil, false, err
	}
	if envelope.Error != nil {
		message := envelope.Error.Message + " " + string(envelope.Error.Data)
		if envelope.Error.Code == -32601 || missingCometHistory(message) {
			return nil, false, nil
		}
		return nil, false, errors.New(message)
	}
	if len(envelope.Result) == 0 || string(envelope.Result) == "null" {
		return nil, false, errors.New("comet returned no capability result")
	}
	succeeded = true
	return envelope.Result, true, nil
}

func missingCometHistory(message string) bool {
	m := strings.ToLower(message)
	return missingEarliestHistory(errors.New(message)) ||
		strings.Contains(m, "is not available, lowest height is") ||
		(strings.Contains(m, "could not find") && strings.Contains(m, "height")) ||
		(strings.Contains(m, "height") && strings.Contains(m, "has been pruned"))
}
