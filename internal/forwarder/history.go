package forwarder

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"

	"github.com/InjectiveLabs/stitch/internal/history"
	"github.com/InjectiveLabs/stitch/internal/types"
)

const maxHistoricalErrorBytes = 64 * 1024

type retainedResponse struct {
	status int
	header http.Header
	body   []byte
}

type replayBody struct {
	io.Reader
	io.Closer
}

type failedRead struct{ err error }

func (r failedRead) Read([]byte) (int, error) { return 0, r.err }

// historicalError peeks only at bounded, structured Cosmos/Comet errors. It
// restores every byte for the normal relay, including over-limit responses.
func historicalError(resp *http.Response, key types.RouteKey) *retainedResponse {
	if !key.Idempotent || key.Class != types.ClassByHeight || key.HeightOrZero() <= 0 {
		return nil
	}
	if key.Protocol != types.ProtoAPI && key.Protocol != types.ProtoRPC {
		return nil
	}
	if key.Protocol == types.ProtoAPI && resp.StatusCode < 400 {
		return nil
	}
	original := resp.Body
	body, err := io.ReadAll(io.LimitReader(original, maxHistoricalErrorBytes+1))
	resp.Body = &replayBody{Reader: io.MultiReader(bytes.NewReader(body), original), Closer: original}
	if err != nil {
		resp.Body = &replayBody{Reader: io.MultiReader(bytes.NewReader(body), failedRead{err}), Closer: original}
	}
	if err != nil || len(body) > maxHistoricalErrorBytes {
		return nil
	}
	var envelope struct {
		Code    json.RawMessage `json:"code"`
		Message string          `json:"message"`
		Error   *struct {
			Message string `json:"message"`
			Data    string `json:"data"`
		} `json:"error"`
		Result struct {
			Response struct {
				Code json.RawMessage `json:"code"`
				Log  string          `json:"log"`
			} `json:"response"`
		} `json:"result"`
	}
	if json.Unmarshal(body, &envelope) != nil {
		return nil
	}
	missing := false
	if key.Protocol == types.ProtoAPI {
		missing = nonzeroCode(envelope.Code) && history.Unavailable(envelope.Message)
	} else if envelope.Error != nil {
		missing = history.Unavailable(envelope.Error.Message + " " + envelope.Error.Data)
	} else if key.Method == "abci_query" {
		missing = nonzeroCode(envelope.Result.Response.Code) && history.Unavailable(envelope.Result.Response.Log)
	}
	if !missing {
		return nil
	}
	return &retainedResponse{status: resp.StatusCode, header: resp.Header.Clone(), body: body}
}

func nonzeroCode(raw json.RawMessage) bool {
	return len(raw) > 0 && string(raw) != "0" && string(raw) != `"0"` && string(raw) != "null"
}
