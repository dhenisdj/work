package work

import (
	"testing"
)

func TestMakeIdentifier(t *testing.T) {
	id := makeIdentifier("test")
	if len(id) < 10 {
		t.Errorf("expected a string of length 10 at least")
	}
}

func TestMakeSuffix(t *testing.T) {
	id := makeSuffix()
	if len(id) < 4 {
		t.Errorf("expected a string of length 4 at least")
	}
}
