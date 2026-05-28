package cache

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
)

// PopulateFromEthResponse parses a successful JSON-RPC response from an
// EVM method and binds any (hash, height) pairs it contains. Tolerant of
// response shapes that omit one or more fields — never errors.
//
// Methods recognized:
//
//   eth_getBlockByNumber, eth_getBlockByHash → result.number + result.hash → ("eth_block", hash) → height
//   eth_getTransactionByHash                  → result.blockNumber + result.hash → ("eth_tx", hash) → height
//   eth_getTransactionReceipt                 → result.blockNumber + result.transactionHash → ("eth_tx", hash) → height
//   eth_getBlockReceipts                      → result[].blockNumber + result[].transactionHash
func PopulateFromEthResponse(idx *HashIndex, method string, body []byte) {
	if idx == nil || len(body) == 0 {
		return
	}
	var env struct {
		Result json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		return
	}
	res := bytes.TrimSpace(env.Result)
	if len(res) == 0 || bytes.Equal(res, []byte("null")) {
		return
	}

	switch method {
	case "eth_getBlockByNumber", "eth_getBlockByHash":
		var block struct {
			Number string `json:"number"`
			Hash   string `json:"hash"`
		}
		if err := json.Unmarshal(res, &block); err != nil {
			return
		}
		h, ok := parseHexInt(block.Number)
		if !ok || block.Hash == "" {
			return
		}
		idx.Set(EthBlockKey(block.Hash), h)

	case "eth_getTransactionByHash":
		var tx struct {
			BlockNumber string `json:"blockNumber"`
			Hash        string `json:"hash"`
			BlockHash   string `json:"blockHash"`
		}
		if err := json.Unmarshal(res, &tx); err != nil {
			return
		}
		h, ok := parseHexInt(tx.BlockNumber)
		if !ok {
			return
		}
		if tx.Hash != "" {
			idx.Set(EthTxKey(tx.Hash), h)
		}
		if tx.BlockHash != "" {
			idx.Set(EthBlockKey(tx.BlockHash), h)
		}

	case "eth_getTransactionReceipt":
		var rec struct {
			BlockNumber     string `json:"blockNumber"`
			BlockHash       string `json:"blockHash"`
			TransactionHash string `json:"transactionHash"`
		}
		if err := json.Unmarshal(res, &rec); err != nil {
			return
		}
		h, ok := parseHexInt(rec.BlockNumber)
		if !ok {
			return
		}
		if rec.TransactionHash != "" {
			idx.Set(EthTxKey(rec.TransactionHash), h)
		}
		if rec.BlockHash != "" {
			idx.Set(EthBlockKey(rec.BlockHash), h)
		}

	case "eth_getBlockReceipts":
		var receipts []struct {
			BlockNumber     string `json:"blockNumber"`
			BlockHash       string `json:"blockHash"`
			TransactionHash string `json:"transactionHash"`
		}
		if err := json.Unmarshal(res, &receipts); err != nil {
			return
		}
		for _, r := range receipts {
			h, ok := parseHexInt(r.BlockNumber)
			if !ok {
				continue
			}
			if r.TransactionHash != "" {
				idx.Set(EthTxKey(r.TransactionHash), h)
			}
			if r.BlockHash != "" {
				idx.Set(EthBlockKey(r.BlockHash), h)
			}
		}
	}
}

// PopulateFromCMTResponse handles CometBFT JSON-RPC bodies.
//
// Methods recognized:
//
//   block, block_by_hash, header, header_by_hash → block.header.height + block_id.hash → ("cmt_block", hash) → height
//   tx                                            → result.height + result.hash → ("cmt_tx", hash) → height
func PopulateFromCMTResponse(idx *HashIndex, method string, body []byte) {
	if idx == nil || len(body) == 0 {
		return
	}
	var env struct {
		Result json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		return
	}
	res := bytes.TrimSpace(env.Result)
	if len(res) == 0 {
		return
	}

	switch method {
	case "block", "block_by_hash":
		var b struct {
			BlockID struct {
				Hash string `json:"hash"`
			} `json:"block_id"`
			Block struct {
				Header struct {
					Height string `json:"height"`
				} `json:"header"`
			} `json:"block"`
		}
		if err := json.Unmarshal(res, &b); err != nil {
			return
		}
		h, ok := parseDecInt(b.Block.Header.Height)
		if !ok || b.BlockID.Hash == "" {
			return
		}
		idx.Set(CMTBlockKey(b.BlockID.Hash), h)

	case "header", "header_by_hash":
		var hd struct {
			Header struct {
				Height string `json:"height"`
			} `json:"header"`
		}
		if err := json.Unmarshal(res, &hd); err != nil {
			return
		}
		// CometBFT header endpoints don't return the header hash in this
		// shape; we'd need block_id from a sibling. Skip caching for now.
		_ = hd

	case "tx":
		var tx struct {
			Hash   string `json:"hash"`
			Height string `json:"height"`
		}
		if err := json.Unmarshal(res, &tx); err != nil {
			return
		}
		h, ok := parseDecInt(tx.Height)
		if !ok || tx.Hash == "" {
			return
		}
		idx.Set(CMTTxKey(tx.Hash), h)
	}
}

// EthBlockKey, EthTxKey, CMTBlockKey, CMTTxKey are the canonical
// namespacing functions. Hashes are normalized to lowercase so case
// differences ("0xABCD" vs "0xabcd") collide on the same entry.
func EthBlockKey(hash string) string { return "eth_block:" + strings.ToLower(hash) }
func EthTxKey(hash string) string    { return "eth_tx:" + strings.ToLower(hash) }
func CMTBlockKey(hash string) string { return "cmt_block:" + strings.ToUpper(hash) }
func CMTTxKey(hash string) string    { return "cmt_tx:" + strings.ToUpper(hash) }

func parseHexInt(s string) (int64, bool) {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X") {
		v, err := strconv.ParseInt(s[2:], 16, 64)
		return v, err == nil
	}
	v, err := strconv.ParseInt(s, 10, 64)
	return v, err == nil
}

func parseDecInt(s string) (int64, bool) {
	v, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	return v, err == nil
}
