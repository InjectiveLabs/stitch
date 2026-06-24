package eth_rpc

import "strings"

// DangerousAllowlist is the per-process set of dangerous methods the
// operator has opted in to. Empty set = absolute deny on every dangerous
// method (the default). A literal "*" entry opts in every dangerous method;
// useful for trusted local deployments.
//
// The allowlist is small enough that linear scan beats a map for the
// common case (operator allows zero or a handful).
type DangerousAllowlist struct {
	allow    []string
	allowAll bool
}

func NewDangerousAllowlist(allow []string) *DangerousAllowlist {
	d := &DangerousAllowlist{}
	for _, method := range allow {
		method = strings.TrimSpace(method)
		switch method {
		case "":
			continue
		case "*":
			d.allowAll = true
		default:
			d.allow = append(d.allow, method)
		}
	}
	return d
}

// Allowed reports whether method is in the allowlist.
func (d *DangerousAllowlist) Allowed(method string) bool {
	if d == nil || method == "" {
		return false
	}
	if d.allowAll {
		return true
	}
	for _, m := range d.allow {
		if m == method {
			return true
		}
	}
	return false
}
