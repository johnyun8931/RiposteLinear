package main

import (
  "testing"

  "henrycg/email/db"
)

func TestArgsZero(t *testing.T) {
  testOnce(t, 0, 0)
}

func TestArgsNonzero(t *testing.T) {
  testOnce(t, 1, 1)
}

func testOnce(t *testing.T, xIdx, yIdx int) {
  var args db.UploadArgs
  var msg db.SlotContents
  msg.Message = [2]byte{123, 101}

  err := initializeUploadArgs(&args, xIdx, yIdx, msg)
  if err != nil {
    t.Fail()
  }

  for serv := 0; serv<len(args.Query); serv++ {
    q := args.Query[serv]
    qDec, err := db.DecryptQuery(serv, q)
    if err != nil {
      t.Fail()
    }

    if !db.ValidateUpload(serv, qDec) {
      t.Fail()
    }
  }
}

