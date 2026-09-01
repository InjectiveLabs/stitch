package cosmos_grpc

import (
	"math"

	"google.golang.org/protobuf/encoding/protowire"
)

// MethodSpec describes a request field whose local chain height Stitch uses
// to select backend coverage for a Cosmos gRPC query. Protobuf payloads carry
// field numbers, not field names, so body-aware routing must be schema-specific.
// Keeping the table exact also prevents unrelated values such as IBC proof
// heights or distribution range filters from being mistaken for the node's
// block height.
type MethodSpec struct {
	HeightField protowire.Number
	HeightName  string
}

// Manifest contains the supported Cosmos height and IBC fee query_height
// request fields. All listed fields use protobuf's varint wire encoding. A
// zero value retains the method's normal "latest" semantics. Deprecated
// aliases, range filters, and counterparty proof heights are intentionally
// excluded.
var Manifest = map[string]MethodSpec{
	"/cosmos.base.tendermint.v1beta1.Service/GetBlockByHeight": {
		HeightField: 1,
		HeightName:  "height",
	},
	"/cosmos.base.tendermint.v1beta1.Service/GetValidatorSetByHeight": {
		HeightField: 1,
		HeightName:  "height",
	},
	"/cosmos.base.tendermint.v1beta1.Service/ABCIQuery": {
		HeightField: 3,
		HeightName:  "height",
	},
	"/cosmos.staking.v1beta1.Query/HistoricalInfo": {
		HeightField: 1,
		HeightName:  "height",
	},
	"/cosmos.tx.v1beta1.Service/GetBlockWithTxs": {
		HeightField: 1,
		HeightName:  "height",
	},

	// IBC fee queries call their local-state selector query_height. The
	// protobuf definitions document it as the block height at which to query.
	"/ibc.applications.fee.v1.Query/IncentivizedPackets": {
		HeightField: 2,
		HeightName:  "query_height",
	},
	"/ibc.applications.fee.v1.Query/IncentivizedPacket": {
		HeightField: 2,
		HeightName:  "query_height",
	},
	"/ibc.applications.fee.v1.Query/IncentivizedPacketsForChannel": {
		HeightField: 4,
		HeightName:  "query_height",
	},
	"/ibc.applications.fee.v1.Query/FeeEnabledChannels": {
		HeightField: 2,
		HeightName:  "query_height",
	},
}

// Lookup returns the body-routing specification for fullMethod.
func Lookup(fullMethod string) (MethodSpec, bool) {
	spec, ok := Manifest[fullMethod]
	return spec, ok
}

// extractRequestHeight reads the manifest-declared scalar from an opaque
// protobuf request. Singular protobuf scalars are last-one-wins, so the scan
// deliberately retains the final correctly-typed occurrence. A syntactically
// valid request with a missing, wrong-wire, or out-of-range value falls back
// to latest routing and is still forwarded unchanged.
func extractRequestHeight(fullMethod string, payload []byte) (int64, bool) {
	spec, ok := Lookup(fullMethod)
	if !ok {
		return 0, false
	}

	var (
		raw   uint64
		found bool
	)
	for len(payload) > 0 {
		num, typ, n := protowire.ConsumeTag(payload)
		if n < 0 {
			return 0, false
		}
		payload = payload[n:]

		if num == spec.HeightField && typ == protowire.VarintType {
			value, m := protowire.ConsumeVarint(payload)
			if m < 0 {
				return 0, false
			}
			raw = value
			found = true
			payload = payload[m:]
			continue
		}

		m := protowire.ConsumeFieldValue(num, typ, payload)
		if m < 0 {
			return 0, false
		}
		payload = payload[m:]
	}

	// RouteKey uses int64. This also rejects negative int64 values, whose
	// two's-complement protobuf representation is greater than MaxInt64.
	if !found || raw == 0 || raw > math.MaxInt64 {
		return 0, false
	}
	return int64(raw), true
}
