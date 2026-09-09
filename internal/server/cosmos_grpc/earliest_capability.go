package cosmos_grpc

// Capability labels remain accepted for gateway compatibility. They never
// choose a different logical archive start or require co-located dependencies.
func validEarliestCapability(capability string) bool {
	switch capability {
	case "state", "block", "execution", "trace", "proof", "storage", "range":
		return true
	default:
		return false
	}
}
