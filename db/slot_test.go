package db

import (
	"crypto/rand"
	"testing"
)

func TestAddRows(t *testing.T) {
	var r1, r2 BitMatrixRow
	if _, err := rand.Read(r1[:]); err != nil {
		t.Fatalf("rand.Read r1 failed: %v", err)
	}
	if _, err := rand.Read(r2[:]); err != nil {
		t.Fatalf("rand.Read r2 failed: %v", err)
	}

	var res BitMatrixRow
	XorRows(&res, &r1)
	XorRows(&res, &r2)

	for i := 0; i < len(r1); i++ {
		if res[i] != r1[i]^r2[i] {
			t.Fatalf("xor mismatch at %d", i)
		}
	}
}

func TestMessageRow(t *testing.T) {
	msg := &Plaintext{X: 2, Y: 5}
	if err := RandomSlot(&msg.Message); err != nil {
		t.Fatalf("RandomSlot failed: %v", err)
	}

	var row BitMatrixRow
	msgRow := MessageToRow(msg)
	XorRows(&row, &msgRow)

	for i := 0; i < len(msg.Message); i++ {
		if row[(SLOT_LENGTH*msg.X)+i] != msg.Message[i] {
			t.Fatalf("message mismatch at byte %d", i)
		}
	}
}

func BenchmarkMessageRow(b *testing.B) {
	var r, s BitMatrixRow
	_, _ = rand.Read(r[:])
	_, _ = rand.Read(s[:])
	for i := 0; i < b.N; i++ {
		XorRows(&r, &s)
	}
}
