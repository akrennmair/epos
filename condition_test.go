package epos

import (
	"testing"
)

var data = map[string]interface{}{
	"foo":  "1",
	"bar":  "2",
	"baz":  "3",
	"quux": 42,
}

func TestTrueAndFalse(t *testing.T) {
	if !(&True{}).Matches(data) {
		t.Error("True doesn't return true")
	}

	if (&False{}).Matches(data) {
		t.Error("False doesn't return false")
	}
}

func TestAnd(t *testing.T) {
	if !(&And{&True{}, &True{}, &True{}}).Matches(data) {
		t.Error("And of Trues doesn't return true")
	}

	if (&And{&True{}, &False{}}).Matches(data) {
		t.Error("And of a True and a False doesn't return false")
	}
}

func TestOr(t *testing.T) {
	if !(&Or{&True{}, &False{}, &True{}, &False{}}).Matches(data) {
		t.Error("Or of Trues and Falses doesn't return true")
	}

	if (&Or{&False{}, &False{}, &False{}, &False{}}).Matches(data) {
		t.Error("Or of Falses only doesn't return false")
	}
}

func TestEquals(t *testing.T) {
	if !(&Equals{"foo", "1"}).Matches(data) {
		t.Error("foo equals 1 failed")
	}

	if (&Equals{"bar", "3"}).Matches(data) {
		t.Error("bar equals 3 return true even though it should be false")
	}

	if !(&Equals{"quux", 42}).Matches(data) {
		t.Error("quux equals 42 failed")
	}
}

func TestComplexQueries(t *testing.T) {
	if !(&And{ &Equals{"foo", "1"}, &Equals{"bar", "2"}}).Matches(data) {
		t.Error("foo = 1 AND bar = 2 failed")
	}

	if !(&Or{ &Equals{"foo", "2"}, &Equals{"quux", 42}}).Matches(data) {
		t.Error("foo = 2 OR quux = 42 failed")
	}

	if (&Or{ &Equals{"foo", "2"}, &Equals{"quux", 23}}).Matches(data) {
		t.Error("foo = 2 OR quux = 23 went ok even though it should have failed")
	}
}
