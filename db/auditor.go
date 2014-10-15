package db

import (
  "bytes"
  "crypto/sha256"
  "encoding/binary"

  "henrycg/email/prf"
  "henrycg/email/utils"
  "henrycg/ffield"
)

type Auditor struct {
}

func (t *Auditor) Audit(args *AuditArgs, reply *AuditReply) error {
  // Bogus for now
  reply.Okay = true
  return nil
}

func sharedSecretVector(uuid int64, queryIdx, itemIdx, serverA, serverB,
    length int) [][ffield.BYTES_PER_FIELD_ELEMENT]byte {
  vec := make([][ffield.BYTES_PER_FIELD_ELEMENT]byte, length)
  str := make([]byte, length * ffield.BYTES_PER_FIELD_ELEMENT)

  // XXX THIS IS UNSAFE -- JUST FOR PERFORMANCE TESTING
  sha_input := new(bytes.Buffer)
  binary.Write(sha_input, binary.BigEndian, utils.SharedSecrets[serverA][serverB][:])
  binary.Write(sha_input, binary.BigEndian, uuid)
  binary.Write(sha_input, binary.BigEndian, queryIdx)
  binary.Write(sha_input, binary.BigEndian, itemIdx)
  binary.Write(sha_input, binary.BigEndian, length)

  key32 := sha256.Sum256(sha_input.Bytes())

  var key16 prf.Key
  copy(key16[:], key32[:])
  gen, _ := prf.NewPrf(key16)
  gen.Evaluate(str)

  block := ffield.BYTES_PER_FIELD_ELEMENT
  for i := 0; i<length; i++ {
    start := i * block
    copy(vec[i][:], str[start:(start+block)])
  }

  return vec
}

func keyTestVector(uuid int64, queryIdx int, serverIdx int,
    query *InsertQuery) [][ffield.BYTES_PER_FIELD_ELEMENT]byte {
  length := len(query.Keys)
  vec := make([][ffield.BYTES_PER_FIELD_ELEMENT]byte, length)

  // Combine bitmask and keys into a single vector called "vec"
  for i := range vec {
    if query.KeyMask[i] {
      vec[i][0] = 0xff
    }
    copy(vec[i][1:], query.Keys[i][:])
  }

  // Compute r and v vectors
  r := sharedSecretVector(uuid, queryIdx, 0, serverIdx, 1 - serverIdx, length)
  v := sharedSecretVector(uuid, queryIdx, 1, serverIdx, 1 - serverIdx, length)

  offset_bytes := sharedSecretVector(uuid, queryIdx, 2, serverIdx, 1 - serverIdx, 1)
  offset, _ := binary.Uvarint(offset_bytes[0][0:8])
  offset %= uint64(length)

  keyTest := make([][ffield.BYTES_PER_FIELD_ELEMENT]byte, length)

  // y1 = ((r*(x1 + v)) << offset)      and      y2 = ((r*(x2 + v)) << offset)
  for i := range vec {
    // Convert vectors into elements of GF(2^256)
    x_elm := ffield.Set(vec[i])
    r_elm := ffield.Set(r[i])
    v_elm := ffield.Set(v[i])

    res := ffield.Mul(r_elm, ffield.Add(x_elm, v_elm))
    keyTest[(i + int(offset)) % length] = ffield.Get(res)
  }

  return keyTest
}

func PrepareAudit(uuid int64, queryIdx int, serverIdx int,
    query *InsertQuery) EncryptedAuditQuery {
  var q AuditQuery
  q.KeyTest = keyTestVector(uuid, queryIdx, serverIdx, query)
  out, err := EncryptAudit(q)
  if err != nil {
    panic("Unexpected error in encryption!")
  }

  return out
}
