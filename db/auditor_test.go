package db

import (
  "testing"

  "code.google.com/p/go.crypto/poly1305"
)

func TestSharedSecret(t *testing.T) {
  L := 16
  v1 := sharedSecretVector(123, 1, 1, 0, 1, L)
  v2 := sharedSecretVector(123, 1, 1, 1, 0, L)
  u1 := sharedSecretVector(456, 1, 1, 0, 1, L)
  u2 := sharedSecretVector(456, 1, 1, 1, 0, L)

  for i := 0; i < L; i++ {
    if v1[i] != v2[i] {
      t.FailNow()
    }

    if u1[i] != u2[i] {
      t.FailNow()
    }

    if v1[i] == u2[i] || v2[i] == u2[i] {
      t.FailNow()
    }
  }
}

func TestVectorDiff(t *testing.T) {
  v1 := make([][poly1305.TagSize]byte, 3)
  v2 := make([][poly1305.TagSize]byte, 3)
  v3 := make([][poly1305.TagSize]byte, 4)

  if !vectorsDifferAtMostOnce(v1, v2) || vectorsDifferAtMostOnce(v1, v3) {
    t.FailNow()
  }

  v1[0][1] = 0xff
  if !vectorsDifferAtMostOnce(v1, v2) {
    t.FailNow()
  }

  v1[2][1] = 0xff
  if vectorsDifferAtMostOnce(v1, v2) {
    t.FailNow()
  }
}


