package db

import (
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

