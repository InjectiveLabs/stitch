package subscription

import "testing"

func TestParseEthKind(t *testing.T) {
	for in, want := range map[string]Kind{
		"newHeads":               KindEthNewHeads,
		`"newHeads"`:             KindEthNewHeads,
		"logs":                   KindEthLogs,
		"newPendingTransactions": KindEthPendingTransactions,
		"syncing":                KindEthSyncing,
		"":                       KindUnknown,
		"makesomethingup":        KindUnknown,
	} {
		if got := ParseEthKind(in); got != want {
			t.Errorf("ParseEthKind(%q) = %v; want %v", in, got, want)
		}
	}
}

func TestKindResumable(t *testing.T) {
	for k, want := range map[Kind]bool{
		KindEthNewHeads:            true,
		KindEthLogs:                true,
		KindEthPendingTransactions: false,
		KindEthSyncing:             false,
		KindUnknown:                false,
	} {
		if got := k.Resumable(); got != want {
			t.Errorf("%v.Resumable() = %v; want %v", k, got, want)
		}
	}
}

func TestCursorOrdering(t *testing.T) {
	a := Cursor{Height: 1}
	b := Cursor{Height: 2}
	c := Cursor{Height: 1, TxIndex: 1}
	d := Cursor{Height: 1, TxIndex: 1, LogIndex: 1}

	if !a.Less(b) {
		t.Error("a < b by height")
	}
	if !a.Less(c) {
		t.Error("a < c by tx_index")
	}
	if !c.Less(d) {
		t.Error("c < d by log_index")
	}
	if a.Less(a) {
		t.Error("a < a should be false")
	}
	if !a.LessEq(a) {
		t.Error("a <= a should be true")
	}
}

func TestCursorAdvance(t *testing.T) {
	var c Cursor
	if !c.Advance(Cursor{Height: 5}) {
		t.Error("zero cursor should advance to 5")
	}
	if c.Height != 5 {
		t.Errorf("after advance: %+v", c)
	}
	if c.Advance(Cursor{Height: 3}) {
		t.Error("retreat should not advance")
	}
	if c.Height != 5 {
		t.Errorf("retreat must not mutate: %+v", c)
	}
	if !c.Advance(Cursor{Height: 6}) {
		t.Error("forward should advance")
	}
	if c.Height != 6 {
		t.Errorf("after second advance: %+v", c)
	}
}
