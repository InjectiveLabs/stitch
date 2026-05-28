package cache

import "testing"

func TestPopulateEthGetBlockByNumber(t *testing.T) {
	h := New(10)
	body := []byte(`{"jsonrpc":"2.0","id":1,"result":{"number":"0x10","hash":"0xABC"}}`)
	PopulateFromEthResponse(h, "eth_getBlockByNumber", body)

	got, ok := h.Get(EthBlockKey("0xabc"))
	if !ok || got != 16 {
		t.Errorf("got %d %v; want 16 true (case-insensitive)", got, ok)
	}
}

func TestPopulateEthGetTransactionByHash(t *testing.T) {
	h := New(10)
	body := []byte(`{"jsonrpc":"2.0","id":1,"result":{"hash":"0xtxhash","blockHash":"0xbh","blockNumber":"0x42"}}`)
	PopulateFromEthResponse(h, "eth_getTransactionByHash", body)

	if got, _ := h.Get(EthTxKey("0xtxhash")); got != 0x42 {
		t.Errorf("tx mapping: %d", got)
	}
	if got, _ := h.Get(EthBlockKey("0xbh")); got != 0x42 {
		t.Errorf("block mapping: %d", got)
	}
}

func TestPopulateEthGetTransactionReceipt(t *testing.T) {
	h := New(10)
	body := []byte(`{"jsonrpc":"2.0","id":1,"result":{"transactionHash":"0xreceipt","blockHash":"0xbh","blockNumber":"0x100"}}`)
	PopulateFromEthResponse(h, "eth_getTransactionReceipt", body)

	if got, _ := h.Get(EthTxKey("0xreceipt")); got != 0x100 {
		t.Errorf("tx hash: %d", got)
	}
	if got, _ := h.Get(EthBlockKey("0xbh")); got != 0x100 {
		t.Errorf("block hash: %d", got)
	}
}

func TestPopulateEthGetBlockReceipts(t *testing.T) {
	h := New(10)
	body := []byte(`{"jsonrpc":"2.0","id":1,"result":[{"transactionHash":"0xa","blockHash":"0xb","blockNumber":"0x5"},{"transactionHash":"0xc","blockHash":"0xb","blockNumber":"0x5"}]}`)
	PopulateFromEthResponse(h, "eth_getBlockReceipts", body)

	if got, _ := h.Get(EthTxKey("0xa")); got != 5 {
		t.Errorf("tx a: %d", got)
	}
	if got, _ := h.Get(EthTxKey("0xc")); got != 5 {
		t.Errorf("tx c: %d", got)
	}
	if got, _ := h.Get(EthBlockKey("0xb")); got != 5 {
		t.Errorf("block b: %d", got)
	}
}

func TestPopulateNullResultIsNoOp(t *testing.T) {
	h := New(10)
	PopulateFromEthResponse(h, "eth_getBlockByNumber", []byte(`{"jsonrpc":"2.0","id":1,"result":null}`))
	if h.Size() != 0 {
		t.Errorf("size: %d (null result must populate nothing)", h.Size())
	}
}

func TestPopulateMalformedIsNoOp(t *testing.T) {
	h := New(10)
	PopulateFromEthResponse(h, "eth_getBlockByNumber", []byte(`not json`))
	if h.Size() != 0 {
		t.Errorf("size: %d (malformed must populate nothing)", h.Size())
	}
}

func TestPopulateUnknownMethodIsNoOp(t *testing.T) {
	h := New(10)
	body := []byte(`{"jsonrpc":"2.0","id":1,"result":{"hash":"0xa","number":"0x1"}}`)
	PopulateFromEthResponse(h, "eth_madeUpMethod", body)
	if h.Size() != 0 {
		t.Errorf("size: %d (unknown method must populate nothing)", h.Size())
	}
}

func TestPopulateCMTBlock(t *testing.T) {
	h := New(10)
	body := []byte(`{"jsonrpc":"2.0","id":1,"result":{"block_id":{"hash":"ABCDEF"},"block":{"header":{"height":"42"}}}}`)
	PopulateFromCMTResponse(h, "block", body)
	if got, ok := h.Get(CMTBlockKey("abcdef")); !ok || got != 42 {
		t.Errorf("cmt block: %d %v", got, ok)
	}
}

func TestPopulateCMTTx(t *testing.T) {
	h := New(10)
	body := []byte(`{"jsonrpc":"2.0","id":1,"result":{"hash":"DEADBEEF","height":"100"}}`)
	PopulateFromCMTResponse(h, "tx", body)
	if got, _ := h.Get(CMTTxKey("deadbeef")); got != 100 {
		t.Errorf("cmt tx: %d", got)
	}
}
