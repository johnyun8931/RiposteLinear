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
  msg, _ := RandomPlain()

  err := InitializeUploadArgs(&args, xIdx, yIdx, msg)
  if err != nil {
    t.Fail()
  }

  for serv := 0; serv<len(args.Query); serv++ {
    q := args.Query[serv]
    _, err := DecryptQuery(serv, q)
    if err != nil {
      t.Fail()
    }
  }
}

