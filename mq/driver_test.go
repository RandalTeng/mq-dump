package mq

import "testing"

type namerStub struct{ Driver }

func (namerStub) DumpName() string { return "orders" }

func TestNamerAssertion(t *testing.T) {
	var d Driver = namerStub{}
	n, ok := d.(Namer)
	if !ok || n.DumpName() != "orders" {
		t.Fatalf("Namer assertion failed: ok=%v", ok)
	}
}
