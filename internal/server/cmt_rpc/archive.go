package cmt_rpc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/InjectiveLabs/stitch/internal/backend"
	"github.com/InjectiveLabs/stitch/internal/config"
	"github.com/InjectiveLabs/stitch/internal/types"
)

const archiveResponseLimit = 64 << 20

var exactEthHashQuery = regexp.MustCompile(`^ethereum_tx\.ethereumTxHash\s*=\s*'(0x[0-9a-fA-F]{64})'$`)

type archiveEnvelope struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   *struct {
		Code    int             `json:"code"`
		Message string          `json:"message"`
		Data    json.RawMessage `json:"data"`
	} `json:"error"`
}

func parseArchiveEnvelope(body []byte) (archiveEnvelope, error) {
	var env archiveEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return env, fmt.Errorf("invalid upstream JSON: %w", err)
	}
	if env.JSONRPC != "2.0" || (env.Error == nil) == (len(env.Result) == 0) {
		return env, errors.New("invalid upstream JSON-RPC envelope")
	}
	return env, nil
}

func archiveParam(r *http.Request, d decoded, name string, index int) string {
	if len(d.body) == 0 {
		return strings.Trim(r.URL.Query().Get(name), `"`)
	}
	var req jsonRPCRequest
	if json.Unmarshal(d.body, &req) != nil {
		return ""
	}
	return paramFromJSON(req.Params, name, index)
}

func archiveObject(r *http.Request, d decoded) (string, bool) {
	switch d.key.Method {
	case "block_by_hash":
		hash := normalizeArchiveHash(string(d.key.Hash))
		return hash, validArchiveHash(hash)
	case "tx_search":
		match := exactEthHashQuery.FindStringSubmatch(strings.TrimSpace(archiveParam(r, d, "query", 0)))
		if len(match) == 2 {
			return normalizeArchiveHash(match[1]), true
		}
	}
	return "", false
}

func archiveHeightMethod(d decoded) bool {
	if d.key.Class != types.ClassByHeight || d.key.HeightOrZero() <= 0 || !d.key.Idempotent {
		return false
	}
	switch d.key.Method {
	case "block", "header", "commit", "block_results", "abci_query", "consensus_params", "validators", "tx_search":
		return true
	}
	return false
}

func (s *Server) serveArchive(w http.ResponseWriter, r *http.Request, d decoded) bool {
	profile, universe, head, enabled := s.fwd.ArchiveView()
	if !enabled {
		return false
	}
	hash, object := archiveObject(r, d)
	if !object && !archiveHeightMethod(d) {
		return false
	}
	if len(r.Header.Values("x-stitch-backend")) > 0 {
		// New archive reads are pinned to a logical height, never a server.
		writeArchiveError(w, d, "physical backend affinity is not supported by archive routing")
		return true
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	r = r.WithContext(ctx)
	if object {
		s.serveArchiveObject(w, r, d, profile, universe, head, hash)
		return true
	}
	candidates := s.fwd.Candidates(d.key)
	if len(candidates) > 256 {
		writeArchiveError(w, d, "historical query exceeds 256 candidates")
		return true
	}
	var last error
	for _, b := range candidates {
		if _, err := s.archiveStatus(r, b, profile); err != nil {
			last = err
			continue
		}
		body, err := s.archiveAttempt(r, b, d.key, d.body, func(body []byte) error {
			if err := validateArchiveHeight(body, d, profile.CosmosChainID); err != nil {
				return err
			}
			if d.key.Method == "abci_query" && archiveParam(r, d, "prove", 3) == "true" {
				return validateArchiveProof(body)
			}
			return nil
		})
		if err == nil {
			writeArchiveReply(w, d, body)
			return true
		}
		last = err
	}
	if last == nil {
		last = errors.New("no eligible backend")
	}
	writeArchiveError(w, d, fmt.Sprintf("history unavailable at height %d: %v", d.key.HeightOrZero(), last))
	return true
}

func (s *Server) archiveAttempt(r *http.Request, b *backend.Backend, key types.RouteKey, body []byte, validate func([]byte) error) ([]byte, error) {
	select {
	case s.archiveSlots <- struct{}{}:
		defer func() { <-s.archiveSlots }()
	case <-r.Context().Done():
		return nil, r.Context().Err()
	}
	limit := int64(archiveResponseLimit)
	if key.Method == "tx_search" {
		limit = 8 << 20
	}
	if key.Method == "status" {
		limit = 64 << 10
	}
	return s.fwd.ValidatedAttempt(r, b, key, body, limit, func(response []byte) error {
		if len(body) > 0 {
			var request jsonRPCRequest
			if json.Unmarshal(body, &request) == nil && len(request.ID) > 0 {
				envelope, err := parseArchiveEnvelope(response)
				if err != nil {
					return err
				}
				if !jsonEqual(request.ID, envelope.ID) {
					return errors.New("upstream JSON-RPC response ID mismatch")
				}
			}
		}
		return validate(response)
	})
}

// Identity and observed head come from the same HTTP endpoint as the read.
// Retained state does not require a historical block on that endpoint.
func (s *Server) archiveStatus(r *http.Request, b *backend.Backend, profile config.ArchiveProfile) (int64, error) {
	probe := r.Clone(r.Context())
	probe.Method = http.MethodGet
	probe.URL = &url.URL{Path: "/status"}
	key := types.RouteKey{Protocol: types.ProtoRPC, Method: "status", Class: types.ClassLatest, Idempotent: true}
	var height int64
	_, err := s.archiveAttempt(probe, b, key, nil, func(body []byte) error {
		env, err := parseArchiveEnvelope(body)
		if err != nil {
			return err
		}
		if env.Error != nil {
			return errors.New("archive status failed")
		}
		var result struct {
			NodeInfo struct {
				Network string `json:"network"`
			} `json:"node_info"`
			SyncInfo struct {
				Height string `json:"latest_block_height"`
			} `json:"sync_info"`
		}
		if err := json.Unmarshal(env.Result, &result); err != nil {
			return err
		}
		height, err = strconv.ParseInt(result.SyncInfo.Height, 10, 64)
		if err != nil || height <= 0 || result.NodeInfo.Network != profile.CosmosChainID {
			return errors.New("archive endpoint returned the wrong chain or invalid head")
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return height, nil
}

func validateArchiveProof(body []byte) error {
	env, err := parseArchiveEnvelope(body)
	if err != nil || env.Error != nil {
		return err
	}
	var result struct {
		Response struct {
			Code  uint32 `json:"code"`
			Proof *struct {
				Ops []struct {
					Type string `json:"type"`
					Data string `json:"data"`
				} `json:"ops"`
			} `json:"proofOps"`
		} `json:"response"`
	}
	if json.Unmarshal(env.Result, &result) != nil {
		return errors.New("invalid ABCI proof response")
	}
	if result.Response.Code != 0 {
		return nil
	}
	if result.Response.Proof == nil || len(result.Response.Proof.Ops) == 0 {
		return errors.New("missing requested ABCI proof")
	}
	for _, op := range result.Response.Proof.Ops {
		if op.Type == "" || op.Data == "" {
			return errors.New("invalid ABCI proof operation")
		}
	}
	return nil
}

func validateArchiveHeight(body []byte, d decoded, chain string) error {
	env, err := parseArchiveEnvelope(body)
	if err != nil {
		return err
	}
	if env.Error != nil {
		// Bad client parameters and unsupported methods are domain outcomes;
		// unavailable history commonly arrives as an HTTP-200 internal error.
		if env.Error.Code == -32602 || env.Error.Code == -32601 {
			return nil
		}
		return fmt.Errorf("upstream RPC error: %s %s", env.Error.Message, env.Error.Data)
	}
	var result map[string]json.RawMessage
	if json.Unmarshal(env.Result, &result) != nil || result == nil {
		return errors.New("missing historical result")
	}
	height := d.key.HeightOrZero()
	switch d.key.Method {
	case "block", "header", "commit":
		h, c, _, err := archiveBlockIdentity(result, d.key.Method)
		if err != nil {
			return err
		}
		if h != height || c != chain {
			return errors.New("historical block height or chain mismatch")
		}
	case "block_results":
		if rawHeight(result["height"]) != height {
			return errors.New("block results height mismatch")
		}
	case "consensus_params", "validators":
		if rawHeight(result["block_height"]) != height {
			return errors.New("historical response height mismatch")
		}
	case "abci_query":
		var response struct {
			Height json.RawMessage `json:"height"`
			Code   uint32          `json:"code"`
			Log    string          `json:"log"`
		}
		if json.Unmarshal(result["response"], &response) != nil {
			return errors.New("missing ABCI response")
		}
		if response.Code != 0 && archiveMissingHistory(response.Log) {
			return errors.New(response.Log)
		}
		// Domain errors do not always carry a height; successful data must.
		if response.Code == 0 && rawHeight(response.Height) != height {
			return errors.New("ABCI response height mismatch")
		}
	case "tx_search":
		var search archiveSearch
		if json.Unmarshal(env.Result, &search) != nil {
			return errors.New("invalid transaction search response")
		}
		count, err := strconv.Atoi(search.Total)
		if _, present := result["txs"]; !present || err != nil || count < len(search.Txs) {
			return errors.New("missing or invalid transaction search count")
		}
		for _, tx := range search.Txs {
			if rawHeight(tx.Height) != height {
				return errors.New("transaction search height mismatch")
			}
			if err := validateCometTx(tx); err != nil {
				return err
			}
		}
	}
	return nil
}

func archiveMissingHistory(message string) bool {
	m := strings.ToLower(message)
	return strings.Contains(m, "failed to load state at height") || strings.Contains(m, "version does not exist") ||
		strings.Contains(m, "version mismatch") || strings.Contains(m, "lowest height") || strings.Contains(m, "height is not available")
}

func rawHeight(value json.RawMessage) int64 {
	h, _ := strconv.ParseInt(unquoteRaw(value), 10, 64)
	return h
}

func archiveBlockIdentity(result map[string]json.RawMessage, method string) (int64, string, string, error) {
	var header struct {
		Height  string `json:"height"`
		ChainID string `json:"chain_id"`
	}
	var block struct {
		Header json.RawMessage `json:"header"`
	}
	var blockID struct {
		Hash string `json:"hash"`
	}
	var raw json.RawMessage
	switch method {
	case "block", "block_by_hash":
		if json.Unmarshal(result["block"], &block) != nil {
			return 0, "", "", errors.New("missing block")
		}
		raw = block.Header
		if json.Unmarshal(result["block_id"], &blockID) != nil || !validArchiveHash(normalizeArchiveHash(blockID.Hash)) {
			return 0, "", "", errors.New("missing block hash")
		}
	case "commit":
		if json.Unmarshal(result["signed_header"], &block) != nil {
			return 0, "", "", errors.New("missing signed header")
		}
		raw = block.Header
	default:
		raw = result["header"]
	}
	if json.Unmarshal(raw, &header) != nil {
		return 0, "", "", errors.New("missing block header")
	}
	height, err := strconv.ParseInt(header.Height, 10, 64)
	if err != nil || height <= 0 || header.ChainID == "" {
		return 0, "", "", errors.New("invalid block header")
	}
	return height, header.ChainID, normalizeArchiveHash(blockID.Hash), nil
}

type archiveTx struct {
	Hash   string          `json:"hash"`
	Height json.RawMessage `json:"height"`
	Index  uint32          `json:"index"`
	Tx     string          `json:"tx"`
	Result json.RawMessage `json:"tx_result"`
	Proof  json.RawMessage `json:"proof,omitempty"`
}

type archiveSearch struct {
	Txs   []archiveTx `json:"txs"`
	Total string      `json:"total_count"`
}

func validateCometTx(tx archiveTx) error {
	raw, err := base64.StdEncoding.DecodeString(tx.Tx)
	if err != nil || len(raw) == 0 {
		return errors.New("invalid transaction bytes")
	}
	hash := sha256.Sum256(raw)
	if normalizeArchiveHash(tx.Hash) != hex.EncodeToString(hash[:]) {
		return errors.New("transaction hash does not match bytes")
	}
	if rawHeight(tx.Height) <= 0 || len(tx.Result) == 0 {
		return errors.New("missing transaction height or result")
	}
	return nil
}

func normalizeArchiveHash(hash string) string {
	hash = strings.TrimPrefix(strings.TrimPrefix(strings.Trim(hash, `"`), "0x"), "0X")
	if len(hash) == 64 {
		if _, err := hex.DecodeString(hash); err == nil {
			return strings.ToLower(hash)
		}
	}
	// Comet's Go BlockByHash client passes []byte, serialized as base64 in
	// JSON-RPC. URI clients and response block IDs normally use hex.
	if raw, err := base64.StdEncoding.DecodeString(hash); err == nil && len(raw) == sha256.Size {
		return hex.EncodeToString(raw)
	}
	return strings.ToLower(hash)
}

func validArchiveHash(hash string) bool {
	if len(hash) != 64 {
		return false
	}
	_, err := hex.DecodeString(hash)
	return err == nil
}

func writeArchiveReply(w http.ResponseWriter, d decoded, body []byte) {
	// Preserve the caller's JSON-RPC id even for internally generated searches.
	var envelope map[string]json.RawMessage
	var request jsonRPCRequest
	if len(d.body) > 0 && json.Unmarshal(d.body, &request) == nil && json.Unmarshal(body, &envelope) == nil {
		envelope["id"] = request.ID
		if encoded, err := json.Marshal(envelope); err == nil {
			body = encoded
		}
	}
	w.Header().Set("content-type", "application/json")
	w.Header().Set("x-content-type-options", "nosniff")
	w.Header().Set("x-stitch-cache", "bypass")
	_, _ = w.Write(body) //nolint:gosec // G705: validated JSON, never HTML; nosniff prevents content reinterpretation.
}

func writeArchiveError(w http.ResponseWriter, d decoded, message string) {
	id := json.RawMessage("null")
	var request jsonRPCRequest
	if len(d.body) > 0 && json.Unmarshal(d.body, &request) == nil && len(request.ID) > 0 {
		id = request.ID
	}
	body, _ := json.Marshal(struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Error   map[string]any  `json:"error"`
	}{"2.0", id, map[string]any{"code": -32000, "message": message}})
	w.Header().Set("content-type", "application/json")
	w.Header().Set("x-content-type-options", "nosniff")
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = w.Write(body)
}

type archiveOutcome struct {
	backend  *backend.Backend
	head     int64
	body     []byte
	txs      []archiveTx
	positive bool
	err      error
}

func (s *Server) serveArchiveObject(w http.ResponseWriter, r *http.Request, d decoded, profile config.ArchiveProfile, universe []*backend.Backend, head int64, hash string) {
	key := d.key
	key.Class = types.ClassEarliest // Archive candidates, including bounded shards.
	key.Height, key.Range = nil, nil
	candidates := s.fwd.Candidates(key)
	if len(universe) > 256 || len(candidates) > 256 {
		writeArchiveError(w, d, "archive search exceeds 256 candidates")
		return
	}
	jobs := make(chan *backend.Backend)
	results := make(chan archiveOutcome, 4)
	var workers sync.WaitGroup
	for i := 0; i < min(4, len(candidates)); i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for b := range jobs {
				results <- s.searchArchiveBackend(r, d, b, profile, hash)
			}
		}()
	}
	go func() {
		defer close(jobs)
		for _, b := range candidates {
			select {
			case jobs <- b:
			case <-r.Context().Done():
				return
			}
		}
	}()
	go func() { workers.Wait(); close(results) }()
	var outcomes []archiveOutcome
	var winner []byte
	var merged []archiveTx
	var mergedBytes int
	seen := make(map[string]archiveTx)
	conflict := false
	for result := range results {
		head = max(head, result.head)
		if result.err == nil && result.positive {
			if d.key.Method == "tx_search" {
				for _, tx := range result.txs {
					identity := normalizeArchiveHash(tx.Hash)
					if prior, ok := seen[identity]; ok {
						if rawHeight(prior.Height) != rawHeight(tx.Height) || prior.Index != tx.Index || !jsonEqual(prior.Result, tx.Result) {
							conflict = true
						}
					} else if len(merged) >= 100 || mergedBytes+len(tx.Tx)+len(tx.Result) > 8<<20 {
						conflict = true
					} else {
						seen[identity] = tx
						merged = append(merged, tx)
						mergedBytes += len(tx.Tx) + len(tx.Result)
					}
				}
			} else if winner == nil {
				winner = result.body
			} else {
				var a, b archiveEnvelope
				_ = json.Unmarshal(winner, &a)
				_ = json.Unmarshal(result.body, &b)
				if !jsonEqual(a.Result, b.Result) {
					conflict = true
				}
			}
		}
		result.body, result.txs = nil, nil
		outcomes = append(outcomes, result)
	}
	if conflict {
		writeArchiveError(w, d, "archive replicas returned conflicting object identities")
		return
	}
	if r.Context().Err() != nil {
		writeArchiveError(w, d, "archive search deadline exceeded")
		return
	}
	if d.key.Method == "tx_search" && len(merged) > 0 {
		// More than one distinct Comet transaction for an exact EVM hash needs
		// gateway decoding, not an arbitrary first result from racing shards.
		if !archiveCoverageComplete(profile.EVMStartHeight, head, universe, outcomes) {
			writeArchiveError(w, d, "archive hash search incomplete")
			return
		}
		body, err := archiveSearchReply(r, d, merged)
		if err != nil {
			writeArchiveError(w, d, err.Error())
			return
		}
		writeArchiveReply(w, d, body)
		return
	}
	if winner != nil {
		writeArchiveReply(w, d, winner)
		return
	}
	if !archiveCoverageComplete(profile.EVMStartHeight, head, universe, outcomes) {
		writeArchiveError(w, d, "archive hash search incomplete: required history was not searched")
		return
	}
	if d.key.Method == "tx_search" {
		body, err := archiveSearchReply(r, d, []archiveTx{})
		if err != nil {
			writeArchiveError(w, d, err.Error())
			return
		}
		writeArchiveReply(w, d, body)
		return
	}
	// A null result is only global after the entire advertised interval was
	// searched. For Comet block lookups retain its native null block shape.
	result := any(nil)
	if d.key.Method == "block_by_hash" {
		result = map[string]any{"block_id": map[string]any{"hash": "", "parts": map[string]any{"total": 0, "hash": ""}}, "block": nil}
	}
	if d.key.Method == "header_by_hash" {
		result = map[string]any{"header": nil}
	}
	body, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": nil, "result": result})
	writeArchiveReply(w, d, body)
}

func jsonEqual(a, b []byte) bool {
	var x, y any
	dx, dy := json.NewDecoder(bytes.NewReader(a)), json.NewDecoder(bytes.NewReader(b))
	dx.UseNumber()
	dy.UseNumber()
	if dx.Decode(&x) != nil || dy.Decode(&y) != nil {
		return false
	}
	xb, _ := json.Marshal(x)
	yb, _ := json.Marshal(y)
	return bytes.Equal(xb, yb)
}

func (s *Server) searchArchiveBackend(r *http.Request, d decoded, b *backend.Backend, profile config.ArchiveProfile, hash string) archiveOutcome {
	out := archiveOutcome{backend: b}
	out.head, out.err = s.archiveStatus(r, b, profile)
	if out.err != nil {
		return out
	}
	upper := min(out.head, b.Coverage.EffectiveUpper(out.head))
	lower := max(profile.EVMStartHeight, b.Coverage.EffectiveLower(out.head))
	if upper < lower {
		return out
	}
	request, body := r, d.body
	if d.key.Method == "tx_search" {
		if prove := archiveParam(r, d, "prove", 1); prove != "" && prove != "false" {
			out.err = errors.New("archive exact-hash search with proofs is not supported")
			return out
		}
		// Exact-hash queries normally return a single item. Bound accidental
		// multiplicity and fetch all results before global pagination.
		query := fmt.Sprintf("ethereum_tx.ethereumTxHash='0x%s' AND tx.height >= %d AND tx.height <= %d", hash, lower, upper)
		body, out.err = json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "tx_search", "params": map[string]any{"query": query, "prove": false, "page": "1", "per_page": "100", "order_by": "asc"}})
		if out.err != nil {
			return out
		}
		request = r.Clone(r.Context())
		request.Method = http.MethodPost
		request.URL = &url.URL{Path: "/"}
	}
	out.body, out.err = s.archiveAttempt(request, b, d.key, body, func(body []byte) error {
		env, err := parseArchiveEnvelope(body)
		if err != nil {
			return err
		}
		if env.Error != nil {
			// Only the well-known object miss is absence. Missing state,
			// unsupported indexes, and generic internal errors stay failures.
			message := strings.ToLower(env.Error.Message + " " + string(env.Error.Data))
			if d.key.Method != "tx_search" && strings.Contains(message, "not found") && strings.Contains(message, hash) {
				return nil
			}
			return fmt.Errorf("archive object lookup failed: %s", message)
		}
		if d.key.Method == "tx_search" {
			var result archiveSearch
			if json.Unmarshal(env.Result, &result) != nil {
				return errors.New("invalid archive search result")
			}
			var fields map[string]json.RawMessage
			if json.Unmarshal(env.Result, &fields) != nil || len(fields["txs"]) == 0 {
				return errors.New("missing archive transaction search items")
			}
			count, err := strconv.Atoi(result.Total)
			if err != nil || count < 0 || count != len(result.Txs) || count > 100 {
				return errors.New("archive exact-hash index search truncated or invalid")
			}
			for _, tx := range result.Txs {
				if err := validateCometTx(tx); err != nil {
					return err
				}
				if h := rawHeight(tx.Height); h < lower || h > upper {
					return errors.New("transaction search escaped shard coverage")
				}
				if !archiveTxEventMatches(tx, hash) {
					return errors.New("transaction search returned another Ethereum hash")
				}
			}
			out.txs, out.positive = result.Txs, len(result.Txs) > 0
			return nil
		}
		if bytes.Equal(bytes.TrimSpace(env.Result), []byte("null")) {
			return nil
		}
		var result map[string]json.RawMessage
		if json.Unmarshal(env.Result, &result) != nil || result == nil {
			return errors.New("invalid archive object response")
		}
		if d.key.Method == "tx" {
			var tx archiveTx
			if json.Unmarshal(env.Result, &tx) != nil {
				return errors.New("invalid transaction response")
			}
			if err := validateCometTx(tx); err != nil {
				return err
			}
			if normalizeArchiveHash(tx.Hash) != hash || rawHeight(tx.Height) < lower || rawHeight(tx.Height) > upper {
				return errors.New("transaction identity or coverage mismatch")
			}
		} else {
			field := "block"
			if d.key.Method == "header_by_hash" {
				field = "header"
			}
			if bytes.Equal(bytes.TrimSpace(result[field]), []byte("null")) {
				return nil
			}
			h, chain, actual, err := archiveBlockIdentity(result, d.key.Method)
			if err != nil {
				return err
			}
			// header_by_hash lacks a block ID; request its block to witness
			// the association rather than trusting an unverified header.
			if d.key.Method == "header_by_hash" {
				return errors.New("header hash lookup requires block_by_hash identity")
			}
			if h < lower || h > upper || chain != profile.CosmosChainID || actual != hash {
				return errors.New("block hash, chain, or coverage mismatch")
			}
		}
		out.positive = true
		return nil
	})
	return out
}

func archiveTxEventMatches(tx archiveTx, hash string) bool {
	var result struct {
		Events []struct {
			Type       string `json:"type"`
			Attributes []struct {
				Key   string `json:"key"`
				Value string `json:"value"`
			} `json:"attributes"`
		} `json:"events"`
	}
	if json.Unmarshal(tx.Result, &result) != nil {
		return false
	}
	for _, event := range result.Events {
		if event.Type != "ethereum_tx" {
			continue
		}
		for _, attr := range event.Attributes {
			if attr.Key == "ethereumTxHash" && normalizeArchiveHash(attr.Value) == hash {
				return true
			}
		}
	}
	return false
}

func archiveCoverageComplete(start, head int64, universe []*backend.Backend, outcomes []archiveOutcome) bool {
	if start <= 0 || head < start || len(universe) == 0 {
		return false
	}
	type interval struct{ low, high int64 }
	var covered []interval
	for _, result := range outcomes {
		if result.err != nil {
			continue
		}
		low := max(start, result.backend.Coverage.EffectiveLower(head))
		high := min(head, result.head, result.backend.Coverage.EffectiveUpper(head))
		if low <= high {
			covered = append(covered, interval{low, high})
		}
	}
	sort.Slice(covered, func(i, j int) bool { return covered[i].low < covered[j].low })
	next := start
	for _, window := range covered {
		if window.low > next {
			return false
		}
		if window.high >= head {
			return true
		}
		next = max(next, window.high+1)
	}
	return false
}

func archiveSearchReply(r *http.Request, d decoded, txs []archiveTx) ([]byte, error) {
	page, perPage := 1, 30
	if p := archiveParam(r, d, "page", 2); p != "" {
		n, err := strconv.Atoi(p)
		if err != nil || n < 1 {
			return nil, errors.New("invalid archive search page")
		}
		page = n
	}
	if p := archiveParam(r, d, "per_page", 3); p != "" {
		n, err := strconv.Atoi(p)
		if err != nil || n < 1 || n > 100 {
			return nil, errors.New("invalid archive search page size")
		}
		perPage = n
	}
	order := archiveParam(r, d, "order_by", 4)
	if order != "" && order != "asc" && order != "desc" {
		return nil, errors.New("invalid archive search order")
	}
	sort.Slice(txs, func(i, j int) bool {
		hi, hj := rawHeight(txs[i].Height), rawHeight(txs[j].Height)
		if hi != hj {
			if order == "desc" {
				return hi > hj
			}
			return hi < hj
		}
		if order == "desc" {
			return txs[i].Index > txs[j].Index
		}
		return txs[i].Index < txs[j].Index
	})
	count := len(txs)
	// Avoid overflowing a client-controlled page multiplication.
	start := count
	if page-1 <= count/perPage {
		start = min(count, (page-1)*perPage)
	}
	end := min(count, start+perPage)
	return json.Marshal(map[string]any{"jsonrpc": "2.0", "id": nil, "result": archiveSearch{Txs: txs[start:end], Total: strconv.Itoa(count)}})
}
