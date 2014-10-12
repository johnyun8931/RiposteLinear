package db

import (
  "bytes"
  "crypto/rand"
  "testing"

  "henrycg/email/prf"
  "henrycg/email/utils"
)

func randomQuery(t *testing.T) InsertQuery {
  var q InsertQuery
  utils.RandomVector(q.KeyMask[:])
  var err error
  for i:=0; i<len(q.Keys); i++ {
    q.Keys[i], err = prf.NewKey()
    if err != nil {
      t.FailNow()
    }
  }

  return q
}

func TestEncryptGood(t *testing.T) {
  for i := 0 ; i < utils.NumServers(); i++ {

    q := randomQuery(t)
    enc, err := EncryptQuery(i, q)
    if err != nil {
      t.Fatal("Could not encrypt")
    }

    dec, err := DecryptQuery(i, enc)
    if err != nil {
      t.Fatal("Decryption: ", err)
    }

    for j := 0; j < len(dec.Keys); j++ {
      if dec.Keys[j] != q.Keys[j] {
        t.Fail()
      }
    }
  }
}


func TestEncryptBad(t *testing.T) {
  for i := 0 ; i < utils.NumServers(); i++ {

    q := randomQuery(t)
    enc, err := EncryptQuery(i, q)
    if err != nil {
      t.Fatal("Could not encrypt")
    }

    _, err = DecryptQuery((i+1)%utils.NumServers(), enc)
    if err == nil {
      t.Fail()
    }
  }
}

func TestEncryptSlot(t *testing.T) {
  var err error
  m := make([]byte, PLAIN_LENGTH)
  _, err = rand.Read(m[:])
  if err != nil {
    t.FailNow()
  }

  c1, err := EncryptSlot(1, m)
  if err != nil {
    t.Fatal("Could not encrypt 1")
  }

  overhead := BOX_PUBLIC_KEY_LEN + BOX_OVERHEAD
  if len(c1) != PLAIN_LENGTH + overhead {
    t.Fatal("Expected len %v, actual %v", PLAIN_LENGTH + overhead, len(c1))
  }

  c2, err := EncryptSlot(0, c1)
  if err != nil {
    t.Fatal("Could not encrypt 0")
  }

  if len(c2) != SLOT_LENGTH {
    t.Fatal("Expected len %v, actual %v", SLOT_LENGTH, len(c2))
  }

  m2, err := DecryptSlot(0, c2)
  if err != nil {
    t.FailNow()
  }

  m1, err:= DecryptSlot(1, m2)
  if err != nil {
    t.FailNow()
  }

  if bytes.Compare(m1, m) != 0 {
    t.FailNow()
  }
}
