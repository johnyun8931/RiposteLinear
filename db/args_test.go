package db

import (
  "testing"
)

func TestArgsZero(t *testing.T) {
  testOnce(t, 0, 0)
}

func TestArgsNonzero(t *testing.T) {
  testOnce(t, 1, 1)
}

func testOnce(t *testing.T, xIdx, yIdx int) {
  var args UploadArgs
  var msg SlotContents
  msg.Message = [SLOT_LENGTH]byte{123, 101}

  err := InitializeUploadArgs(&args, xIdx, yIdx, msg)
  if err != nil {
    t.Fail()
  }

  for serv := 0; serv<len(args.Query); serv++ {
    q := args.Query[serv]
    qDec, err := DecryptQuery(serv, q)
    if err != nil {
      t.Fail()
    }

    if !ValidateUpload(serv, qDec) {
      t.Fail()
    }
  }
}

