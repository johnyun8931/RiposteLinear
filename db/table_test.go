package db

import (
	"math/big"
	"testing"
)

func TestSimple(t *testing.T) {
	tab := NewSlotTable()
	tab.ForeachRow(func(_ int, value *BitMatrixRow) {
		for i := 0; i < len(value); i++ {
			value[i] = 2
		}
	})

	if tab.table[0][0] != 2 {
		t.Fatal("expected table to be populated")
	}

	tab.Clear()

	if tab.table[0][0] != 0 {
		t.Fatal("expected table to be cleared")
	}
}

func TestEndToEndNoProof(t *testing.T) {
	msg, err := RandomMessage()
	if err != nil {
		t.Fatalf("RandomMessage failed: %v", err)
	}

	var args UploadArgs1
	if _, err := InitializeUploadArgs(&args, msg, false); err != nil {
		t.Fatalf("InitializeUploadArgs failed: %v", err)
	}

	slotTables := make([]*SlotTable, NUM_SERVERS)
	for i := 0; i < NUM_SERVERS; i++ {
		slotTables[i] = NewSlotTable()

		var query InsertQuery1
		if err := DecryptQuery(i, args.Query[i], &query); err != nil {
			t.Fatalf("DecryptQuery(%d) failed: %v", i, err)
		}

		tup := &InsertQueryTuple{q1: query}
		reply := &PrepareReply{}
		slotTables[i].processQuery(tup, reply, i == 1, zeroBigInt(), zeroBigInt())
	}

	var replies [NUM_SERVERS]DumpReply
	for i := 0; i < NUM_SERVERS; i++ {
		replies[i].Entries = new(BitMatrix)
		slotTables[i].CopyToAndClear(replies[i].Entries)
	}

	b := revealCleartext(replies)
	var out SlotContents
	start := SLOT_LENGTH * msg.X
	copy(out[:], b[msg.Y][start:start+SLOT_LENGTH])
	if out != msg.Message {
		t.Fatalf("message mismatch: got %x want %x", out, msg.Message)
	}
}

func BenchmarkTable(b *testing.B) {
	msg := testPlaintext(3, 5, "benchmark")

	var args UploadArgs1
	if _, err := InitializeUploadArgs(&args, msg, false); err != nil {
		b.Fatalf("InitializeUploadArgs failed: %v", err)
	}

	var query InsertQuery1
	if err := DecryptQuery(0, args.Query[0], &query); err != nil {
		b.Fatalf("DecryptQuery failed: %v", err)
	}

	slotTable := NewSlotTable()
	reply := &PrepareReply{}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		slotTable.processQuery(&InsertQueryTuple{q1: query}, reply, false, zeroBigInt(), zeroBigInt())
	}
}

func zeroBigInt() *big.Int {
	return new(big.Int)
}
