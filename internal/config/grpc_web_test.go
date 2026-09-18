package config

import "testing"

func TestGRPCWebRequiresNativeListener(t *testing.T) {
	c := diffFixture()
	applyDefaults(c)
	c.Listen.GRPCWeb = GRPCWebConfig{Addr: ":5004", AllowedOrigins: []string{"https://app.example.com"}}
	if Validate(c) == nil {
		t.Fatal("web listener without native server accepted")
	}
	c.Listen.GRPC = AddrConfig{Addr: ":5002"}
	if err := Validate(c); err != nil {
		t.Fatal(err)
	}
	next := *c
	next.Listen.GRPCWeb.AllowedOrigins = []string{"https://other.example.com"}
	diff := DiffNonReloadable(c, &next)
	if len(diff) != 1 || diff[0] != "listen" {
		t.Fatalf("origin changes must require restart: %v", diff)
	}
}
