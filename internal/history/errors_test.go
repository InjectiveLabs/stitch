package history

import "testing"

func TestUnavailable(t *testing.T) {
	for _, s := range []string{
		"failed to load state at height 105504992; version mismatch on immutable IAVL tree; version does not exist",
		"no commit info found for version 112637000",
		"failed to load version 20: version does not exist",
	} {
		if !Unavailable(s) {
			t.Errorf("unrecognized retention failure: %s", s)
		}
	}
	for _, s := range []string{"account does not exist", "permission denied", "invalid request", "version does not exist", "iavl query successful"} {
		if Unavailable(s) {
			t.Errorf("ordinary error classified as retention failure: %s", s)
		}
	}
}
