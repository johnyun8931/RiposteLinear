package db

import (
  "testing"
  "henrycg/email/utils"
)

func TestEncryptGood(t *testing.T) {
  for i := 0 ; i < utils.NumServers(); i++ {
    var q InsertQuery
    utils.RandomVector(q.XCoords[:])

    enc, err := EncryptQuery(i, q)
    if err != nil {
      t.Fatal("Could not encrypt")
    }

    dec, err := DecryptQuery(i, enc)
    if err != nil {
      t.Fatal("Decryption: ", err)
    }

    for j := 0; j < len(dec.XCoords); j++ {
      if dec.XCoords[j] != q.XCoords[j] {
        t.Fail()
      }
    }
  }
}

func TestEncryptBad(t *testing.T) {
  for i := 0 ; i < utils.NumServers(); i++ {
    var q InsertQuery
    utils.RandomVector(q.XCoords[:])

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

