package cosmos_grpc

import "testing"

func TestHistoricalReadContractsCoverChainModules(t *testing.T) {
	for _, method := range []string{
		"/cosmos.bank.v1beta1.Query/Balance",
		"/cosmos.staking.v1beta1.Query/Delegation",
		"/cosmos.distribution.v1beta1.Query/DelegationRewards",
		"/cosmos.authz.v1beta1.Query/Grants",
		"/cosmos.gov.v1.Query/Proposal",
		"/ibc.core.client.v1.Query/ClientState",
		"/ibc.core.channel.v1.Query/Channel",
		"/ibc.applications.transfer.v1.Query/DenomTrace",
		"/injective.exchange.v1beta1.Query/SpotMarkets",
		"/injective.exchange.v2.Query/SpotMarkets",
		"/injective.oracle.v1beta1.Query/Params",
		"/injective.evm.v1.Query/Balance",
		"/cosmwasm.wasm.v1.Query/SmartContractState",
	} {
		if !historicalReadMethod(method) {
			t.Errorf("verified chain query excluded: %s", method)
		}
	}
}

func TestHistoricalRetryNeverInfersReadContractFromNamespace(t *testing.T) {
	for _, method := range []string{
		"/cosmos.bank.v1beta1.Query/FutureMethod",
		"/cosmos.unknown.v1.Query/Watch",
		"/injective.exchange.v2.Query/FutureStreamingMethod",
		"/cosmos.bank.v1beta1.Msg/Send",
		"/cosmos.tx.v1beta1.Service/BroadcastTx",
		"/cosmos.tx.v1beta1.Service/Simulate",
		"/ibc.core.channel.v1.Msg/RecvPacket",
		"/injective.exchange.v2.Msg/CreateSpotMarketOrder",
		"/injective.stream.v1beta1.Stream/Stream",
		"/injective.stream.v2.Stream/StreamV2",
		"/injective_exchange_rpc.InjectiveExchangeRPC/StreamOrders",
		"/cosmwasm.wasm.v1.Msg/ExecuteContract",
		"/cosmos.group.v1.Query/GroupInfo",
	} {
		if historicalReadMethod(method) {
			t.Errorf("unverified or non-read RPC is retryable: %s", method)
		}
	}
}

// Adding a protobuf body-height decoder must not silently confer retryability
// on a method whose unary read contract was never checked against its schema.
func TestBodyHeightManifestHasVerifiedUnaryContracts(t *testing.T) {
	for method := range Manifest {
		if !historicalReadMethod(method) {
			t.Errorf("body-height method needs schema verification: %s", method)
		}
	}
}
