package db

import (
	"crypto/rand"
	"math/big"
	"testing"

	"bitbucket.org/henrycg/riposte/mulproof"
	"bitbucket.org/henrycg/riposte/prf"
	"bitbucket.org/henrycg/riposte/utils"
)

func randomInsertQuery1(t *testing.T, keyIndex int) InsertQuery1 {
	t.Helper()

	var q InsertQuery1
	q.KeyIndex = keyIndex
	utils.RandVectorBool(q.KeyMask[:])
	if err := randomVectorKeys(q.Keys[:]); err != nil {
		t.Fatalf("randomVectorKeys failed: %v", err)
	}
	var row BitMatrixRow
	if _, err := rand.Read(row[:]); err != nil {
		t.Fatalf("rand row failed: %v", err)
	}
	q.MessageMask = row
	return q
}

func randomInsertQuery2() InsertQuery2 {
	return InsertQuery2{MsgShare: big.NewInt(12345)}
}

func randomInsertQuery3() InsertQuery3 {
	return InsertQuery3{
		TShare1: big.NewInt(11),
		TShare2: big.NewInt(22),
		TProof1: mulproof.ProofShare{},
		TProof2: mulproof.ProofShare{},
	}
}

func TestEncryptQueryRoundTripInsertQuery1(t *testing.T) {
	for i := 0; i < NUM_SERVERS; i++ {
		q := randomInsertQuery1(t, i)
		enc, err := EncryptQuery(i, &q)
		if err != nil {
			t.Fatalf("EncryptQuery failed: %v", err)
		}

		var dec InsertQuery1
		if err := DecryptQuery(i, enc, &dec); err != nil {
			t.Fatalf("DecryptQuery failed: %v", err)
		}
		if dec.KeyIndex != q.KeyIndex {
			t.Fatalf("expected KeyIndex=%d, got %d", q.KeyIndex, dec.KeyIndex)
		}
		for j := range dec.Keys {
			if dec.Keys[j] != q.Keys[j] {
				t.Fatalf("key mismatch at %d", j)
			}
		}
	}
}

func TestEncryptQueryRoundTripInsertQuery2And3(t *testing.T) {
	q2 := randomInsertQuery2()
	enc2, err := EncryptQuery(0, &q2)
	if err != nil {
		t.Fatalf("EncryptQuery insert query 2 failed: %v", err)
	}
	var dec2 InsertQuery2
	if err := DecryptQuery(0, enc2, &dec2); err != nil {
		t.Fatalf("DecryptQuery insert query 2 failed: %v", err)
	}
	if dec2.MsgShare == nil || dec2.MsgShare.Cmp(q2.MsgShare) != 0 {
		t.Fatalf("unexpected MsgShare %v", dec2.MsgShare)
	}

	q3 := randomInsertQuery3()
	enc3, err := EncryptQuery(1, &q3)
	if err != nil {
		t.Fatalf("EncryptQuery insert query 3 failed: %v", err)
	}
	var dec3 InsertQuery3
	if err := DecryptQuery(1, enc3, &dec3); err != nil {
		t.Fatalf("DecryptQuery insert query 3 failed: %v", err)
	}
	if dec3.TShare1 == nil || dec3.TShare1.Cmp(q3.TShare1) != 0 {
		t.Fatalf("unexpected TShare1 %v", dec3.TShare1)
	}
	if dec3.TShare2 == nil || dec3.TShare2.Cmp(q3.TShare2) != 0 {
		t.Fatalf("unexpected TShare2 %v", dec3.TShare2)
	}
}

func TestEncryptQueryWrongServerFails(t *testing.T) {
	key, err := prf.NewKey()
	if err != nil {
		t.Fatalf("prf.NewKey failed: %v", err)
	}
	q := InsertQuery1{
		KeyIndex: 0,
		Keys:     [TABLE_HEIGHT]prf.Key{key},
	}

	for i := 0; i < NUM_SERVERS; i++ {
		enc, err := EncryptQuery(i, &q)
		if err != nil {
			t.Fatalf("EncryptQuery failed: %v", err)
		}

		var dec InsertQuery1
		err = DecryptQuery((i+1)%NUM_SERVERS, enc, &dec)
		if err == nil {
			t.Fatalf("expected decrypt failure for wrong server %d", (i+1)%NUM_SERVERS)
		}
	}
}
