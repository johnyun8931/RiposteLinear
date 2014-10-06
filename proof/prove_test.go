package proof

import (
  "crypto/rand"

  "testing"
)

func TestProofOkay(t *testing.T) {
  length := 10
  strLen := 2048
  vecA := make([][]byte, length)
  vecB := make([][]byte, length)
  differAt := 4

  for i := 0; i<length; i++ {
    vecA[i] = make([]byte, strLen)
    vecB[i] = make([]byte, strLen)
    rand.Read(vecA[i][:])

    if i == differAt {
      rand.Read(vecB[i][:])
    } else {
      copy(vecB[i][:], vecA[i][:])
    }
  }

  // Make proof
  ev, commitA, secA, commitB, secB := VectorProve(vecA, vecB, differAt)

  if !VectorVerify(vecA, secA, commitB, ev, true) {
    t.Fail()
  }

  if !VectorVerify(vecB, secB, commitA, ev, false) {
    t.Fail()
  }
}

func TestProofFail(t *testing.T) {
  length := 10
  strLen := 2048
  vecA := make([][]byte, length)
  vecB := make([][]byte, length)
  differAt := 4

  for i := 0; i<length; i++ {
    vecA[i] = make([]byte, strLen)
    vecB[i] = make([]byte, strLen)
    rand.Read(vecA[i][:])

    if i == differAt {
      rand.Read(vecB[i][:])
    } else {
      copy(vecB[i][:], vecA[i][:])
    }
  }

  // Make proof
  ev, commitA, secA, commitB, secB := VectorProve(vecA, vecB, differAt)

  if VectorVerify(vecA, secA, commitB, ev, false) {
    t.Fatal("Should not pass 0")
  }

  if VectorVerify(vecB, secB, commitA, ev, true) {
    t.Fatal("Should not pass 1")
  }

  if VectorVerify(vecB, secA, commitA, ev, false) {
    t.Fatal("Should not pass 2")
  }

  if VectorVerify(vecA, secB, commitA, ev, true) {
    t.Fatal("Should not pass 3")
  }

  if VectorVerify(vecA, secA, commitA, ev, true) {
    t.Fatal("Should not pass 4")
  }

  // Make proof
  rand.Read(vecA[0][:])
  ev, commitA, secA, commitB, secB = VectorProve(vecA, vecB, 0)
  if VectorVerify(vecB, secB, commitA, ev, false) {
    t.Fatal("Should not pass 5")
  }
}

func BenchmarkProof(b *testing.B) {
  length := b.N
  strLen := 2048
  vecA := make([][]byte, length)
  vecB := make([][]byte, length)
  differAt := 0

  for i := 0; i<length; i++ {
    vecA[i] = make([]byte, strLen)
    vecB[i] = make([]byte, strLen)
    rand.Read(vecA[i][:])

    if i == differAt {
      rand.Read(vecB[i][:])
    } else {
      copy(vecB[i][:], vecA[i][:])
    }
  }

  // Make proof
  ev, _, secA, commitB, _ := VectorProve(vecA, vecB, differAt)

  b.ResetTimer()
  VectorVerify(vecA, secA, commitB, ev, true)
}
