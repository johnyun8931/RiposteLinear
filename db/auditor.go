package db

import (
  "bytes"
  "crypto/sha256"
  "encoding/binary"
  "log"

  "code.google.com/p/go.crypto/poly1305"

  "henrycg/email/prf"
  "henrycg/email/utils"
)

const MAC_KEY_SIZE = 32

type Auditor struct {
}

type auditResult struct {
  isGood bool
  idx int
}

func (t *Auditor) auditOnce(idx int, c chan auditResult, queries *[NUM_SERVERS]EncryptedAuditQuery) {
  var ret auditResult
  ret.idx = idx
  ret.isGood = false

  var err1, err2 error
  q0, err1 := DecryptAudit(queries[0])
  q1, err2 := DecryptAudit(queries[1])
  if (err1 == nil) && (err2 == nil) {
    ret.isGood = validateQueries(q0, q1)
  } else {
    log.Printf("Audit failed because of decryption failure")
  }
  c<- ret
}

func (t *Auditor) Audit(args *AuditArgs, reply *AuditReply) error {
  reply.Okay = make([]bool, len(args.QueriesToAudit))
  c := make(chan auditResult, len(args.QueriesToAudit))

  for i := range args.QueriesToAudit {
    go t.auditOnce(i, c, &args.QueriesToAudit[i])
  }

  for _ = range args.QueriesToAudit {
    res := <-c
    reply.Okay[res.idx] = res.isGood
    if !res.isGood {
      log.Printf("Audit failed at uuid %v[%v]", args.Uuid, res.idx)
    }
  }

  return nil
}

func sharedSecretVector(uuid int64, queryIdx, itemIdx, serverA, serverB,
    length int) [][MAC_KEY_SIZE]byte {
  vec := make([][MAC_KEY_SIZE]byte, length)
  str := make([]byte, length * MAC_KEY_SIZE)

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

  block := MAC_KEY_SIZE
  for i := 0; i<length; i++ {
    start := i * block
    copy(vec[i][:], str[start:(start+block)])
  }

  return vec
}

func keyTestVector(uuid int64, queryIdx int, serverIdx int,
    query *InsertQuery) [][poly1305.TagSize]byte {
  length := len(query.Keys)
  vec := make([][]byte, length)

  // Combine bitmask and keys into a single vector called "vec"
  for i := range vec {
    vec[i] = make([]byte, prf.KEY_LENGTH + 1)
    if query.KeyMask[i] {
      vec[i][0] = 0xff
    }
    copy(vec[i][1:], query.Keys[i][:])
  }

  return macVector(uuid, queryIdx, serverIdx, vec)
}

func msgTestVector(uuid int64, queryIdx int, serverIdx int,
    query *InsertQuery, genRows *BitMatrixRow) [][poly1305.TagSize]byte {
  var row BitMatrixRow

  if serverIdx > 0 {
    copy(row[:], query.MessageMask[:])
  }

  XorRows(&row, genRows)

  // Divide up the one long []byte into chunks of
  // size SLOT_LENGTH
  vec := make([][]byte, TABLE_WIDTH)
  for i := range vec {
    vec[i] = make([]byte, SLOT_LENGTH)
    start := i * SLOT_LENGTH
    copy(vec[i], row[start:(start+SLOT_LENGTH)])
  }

  return macVector(uuid, queryIdx, serverIdx, vec)
}

func macVector(uuid int64, queryIdx int, serverIdx int, vec_to_mac [][]byte) [][poly1305.TagSize]byte {
  length := len(vec_to_mac)

  // Compute shared secret keys for MAC
  keys := sharedSecretVector(uuid, queryIdx, 2, serverIdx, 1 - serverIdx, length)
  offset_bytes := sharedSecretVector(uuid, queryIdx, 3, serverIdx, 1 - serverIdx, 1)
  offset, _ := binary.Uvarint(offset_bytes[0][0:8])
  offset %= uint64(length)

  out := make([][poly1305.TagSize]byte, length)

  // y1 = (H_k(m1) << offset)      and      y2 = (H_k(m2) << offset)
  var mac_value [poly1305.TagSize]byte
  for i := range vec_to_mac {
    // Convert vectors into elements of GF(2^256)
    poly1305.Sum(&mac_value, vec_to_mac[i][:], &keys[i])
    out[(i + int(offset)) % length] = mac_value
  }

  return out
}

func prepareAudit(uuid int64, queryIdx int, serverIdx int,
    query *InsertQuery, genRows *BitMatrixRow) EncryptedAuditQuery {
  var q AuditQuery

  q.KeyTest = keyTestVector(uuid, queryIdx, serverIdx, query)
  q.MsgTest = msgTestVector(uuid, queryIdx, serverIdx, query, genRows)

  out, err := EncryptAudit(q)
  if err != nil {
    panic("Unexpected error in encryption!")
  }

  return out
}

func validateQueries(q1, q2 *AuditQuery) bool {
  if len(q1.KeyTest) != TABLE_HEIGHT {
    log.Printf("Audit failed because KeyTest vector has wrong length")
    return false
  }

  if len(q1.MsgTest) != TABLE_WIDTH {
    log.Printf("Audit failed because MsgTest vector has wrong length")
    return false
  }

  b1 := vectorsDifferAtMostOnce(q1.KeyTest, q2.KeyTest)
  b2 := vectorsDifferAtMostOnce(q1.MsgTest, q2.MsgTest)

  if !b1 {
    log.Printf("Audit failed because KeyTest vectors differ too often")
  }
  if !b2 {
    log.Printf("Audit failed because MsgTest vectors differ too often")
  }

  return b1 && b2
}

func vectorsDifferAtMostOnce(v1, v2 [][poly1305.TagSize]byte) bool {
  if len(v1) != len(v2) {
    return false
  }

  seen := false
  for i := range v1 {
    // Require that all but one of the entries are equal
    if bytes.Compare(v1[i][:], v2[i][:]) != 0 {
      //log.Printf("Not equal %v %v", e1, e2)
      if seen {
        return false
      } else {
        seen = true
      }
    }
  }

  return true
}
