package db

import (
  "crypto/rand"
  "testing"
)

func TestArgsZeroNoProof(t *testing.T) {
  testOnce(t, 0, 0, false)
}

func TestArgsZeroProof(t *testing.T) {
  testOnce(t, 0, 0, true)
}

func TestArgsNonzeroNoProof(t *testing.T) {
  testOnce(t, 1, 1, false)
}

func TestArgsNonzeroProof(t *testing.T) {
  testOnce(t, 1, 1, true)
}

func testOnce(t *testing.T, xIdx, yIdx int, doProof bool) {
  var args UploadArgs
  var msg SlotContents
  for i := 0; i < len(msg); i++ {
    m, _ := rand.Int(rand.Reader, ORDER)
    msg[i] = *m
  }

  err := InitializeUploadArgs(&args, xIdx, yIdx, msg, doProof)
  if err != nil {
    t.Fail()
  }

  for serv := 0; serv<len(args.Query); serv++ {
    q := args.Query[serv]
    qDec, err := DecryptQuery(serv, q)
    if err != nil {
      t.Fail()
    }

    if doProof && !ValidateUpload(serv, qDec) {
      t.Fail()
    }
  }
}

func TestOnceBadProof(t *testing.T) {
  var args UploadArgs
  var msg SlotContents
  for i := 0; i < len(msg); i++ {
    m, _ := rand.Int(rand.Reader, ORDER)
    msg[i] = *m
  }

  err := InitializeUploadArgs(&args, 1, 1, msg, true)
  if err != nil {
    t.Fail()
  }


  for serv := 0; serv<len(args.Query); serv++ {
    q := args.Query[serv]
    qDec, err := DecryptQuery(serv, q)
    if err != nil {
      t.Fail()
    }

    qDec.CommitsA[2] = qDec.CommitsB[1]

    if ValidateUpload(serv, qDec) {
      t.Fail()
    }
  }
}
