package eth_rpc

import (
	"encoding/json"
	"testing"

	"github.com/decentrio/stitch/internal/types"
)

func TestDecodeRoutesEthGetBalance(t *testing.T) {
	body := []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0xabcd","0x1000"]}`)
	d, _, err := decodeOne(DefaultManifest, body)
	if err != nil {
		t.Fatal(err)
	}
	if d.method != "eth_getBalance" {
		t.Errorf("method: %s", d.method)
	}
	if d.key.Class != types.ClassByHeight {
		t.Errorf("class: %s", d.key.Class)
	}
	if d.key.HeightOrZero() != 0x1000 {
		t.Errorf("height: %#x", d.key.HeightOrZero())
	}
	if !d.key.Idempotent || !d.key.Cacheable {
		t.Errorf("flags: idemp=%v cache=%v", d.key.Idempotent, d.key.Cacheable)
	}
}

func TestDecodeRoutesEthCallWithStateOverrideDisablesCache(t *testing.T) {
	body := []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_call","params":[{"to":"0xabcd"},"0x1000",{"0xfeed":{"balance":"0x1"}}]}`)
	d, _, err := decodeOne(DefaultManifest, body)
	if err != nil {
		t.Fatal(err)
	}
	if d.key.Class != types.ClassByHeight {
		t.Errorf("class: %s", d.key.Class)
	}
	if d.key.HeightOrZero() != 0x1000 {
		t.Errorf("height: %#x", d.key.HeightOrZero())
	}
	if d.key.Cacheable {
		t.Error("state override must disable caching")
	}
}

func TestDecodeRoutesEthGetBlockByHash(t *testing.T) {
	hash := "0x" + repeat("ab", 32)
	body := []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByHash","params":["` + hash + `",false]}`)
	d, _, err := decodeOne(DefaultManifest, body)
	if err != nil {
		t.Fatal(err)
	}
	if d.key.Class != types.ClassByHash {
		t.Errorf("class: %s", d.key.Class)
	}
	if string(d.key.Hash) != hash {
		t.Errorf("hash: %s", d.key.Hash)
	}
}

func TestDecodeRoutesEthGetBalanceWithBlockHashOrNumberObject(t *testing.T) {
	hash := "0x" + repeat("cd", 32)
	body := []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0xabcd",{"blockHash":"` + hash + `"}]}`)
	d, _, err := decodeOne(DefaultManifest, body)
	if err != nil {
		t.Fatal(err)
	}
	if d.key.Class != types.ClassByHash {
		t.Errorf("class: %s", d.key.Class)
	}
	if string(d.key.Hash) != hash {
		t.Errorf("hash: %s", d.key.Hash)
	}
}

func TestDecodeRoutesBroadcast(t *testing.T) {
	body := []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_sendRawTransaction","params":["0xf86c..."]}`)
	d, _, err := decodeOne(DefaultManifest, body)
	if err != nil {
		t.Fatal(err)
	}
	if d.key.Class != types.ClassBroadcast {
		t.Errorf("class: %s", d.key.Class)
	}
	if d.key.Idempotent {
		t.Error("broadcast must not be idempotent")
	}
}

func TestDecodeRejectsSubscribe(t *testing.T) {
	body := []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["newHeads"]}`)
	d, _, err := decodeOne(DefaultManifest, body)
	if err != nil {
		t.Fatal(err)
	}
	if d.fatal == nil {
		t.Fatal("expected fatal error on subscribe over HTTP")
	}
	if d.fatal.code != -32601 {
		t.Errorf("code: %d", d.fatal.code)
	}
}

func TestDecodeFilterMintAndFollow(t *testing.T) {
	mint := []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_newFilter","params":[{}]}`)
	d, _, err := decodeOne(DefaultManifest, mint)
	if err != nil {
		t.Fatal(err)
	}
	if !d.expectFilterMint {
		t.Error("eth_newFilter should set expectFilterMint")
	}

	follow := []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_getFilterChanges","params":["0xdeadbeef"]}`)
	d, _, err = decodeOne(DefaultManifest, follow)
	if err != nil {
		t.Fatal(err)
	}
	if d.followFilterID != "0xdeadbeef" {
		t.Errorf("followFilterID: %s", d.followFilterID)
	}
}

func TestDecodeUnknownMethodIsLatestNonIdempotent(t *testing.T) {
	body := []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_definitelyMadeUp","params":[]}`)
	d, _, err := decodeOne(DefaultManifest, body)
	if err != nil {
		t.Fatal(err)
	}
	if d.key.Class != types.ClassLatest {
		t.Errorf("class: %s", d.key.Class)
	}
	if d.key.Idempotent {
		t.Error("unknown methods must not be retryable")
	}
}

func TestDecodeStateless(t *testing.T) {
	body := []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}`)
	d, _, err := decodeOne(DefaultManifest, body)
	if err != nil {
		t.Fatal(err)
	}
	if d.key.Class != types.ClassStateless {
		t.Errorf("class: %s", d.key.Class)
	}
}

func TestDecodeRawIDPreserved(t *testing.T) {
	body := []byte(`{"jsonrpc":"2.0","id":42,"method":"eth_blockNumber","params":[]}`)
	d, _, err := decodeOne(DefaultManifest, body)
	if err != nil {
		t.Fatal(err)
	}
	var id int
	if err := json.Unmarshal(d.id, &id); err != nil || id != 42 {
		t.Errorf("id parse: %v %d", err, id)
	}
}
