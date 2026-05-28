package eth_rpc

// DangerousAllowlist is the per-process set of dangerous methods the
// operator has opted in to. Empty set = absolute deny on every dangerous
// method (the default).
//
// The allowlist is small enough that linear scan beats a map for the
// common case (operator allows zero or a handful).
type DangerousAllowlist struct {
	allow []string
}

func NewDangerousAllowlist(allow []string) *DangerousAllowlist {
	cp := make([]string, len(allow))
	copy(cp, allow)
	return &DangerousAllowlist{allow: cp}
}

// Allowed reports whether method is in the allowlist.
func (d *DangerousAllowlist) Allowed(method string) bool {
	if d == nil {
		return false
	}
	for _, m := range d.allow {
		if m == method {
			return true
		}
	}
	return false
}
