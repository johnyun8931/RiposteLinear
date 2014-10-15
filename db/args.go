package db

import (
  "log"
  "math/big"

  "henrycg/email/prf"
  "henrycg/email/proof"
  "henrycg/email/utils"
  "henrycg/zkp/group"
  "henrycg/zkp/schnorr"
)

var curve = utils.CommonCurve


func InitializeUploadArgs(args *UploadArgs, xIdx int, yIdx int,
    msg SlotContents, corrupted bool) error {

  // Create random values for secret sharing
  var keys [TABLE_HEIGHT]prf.Key
  var keysP [TABLE_HEIGHT]prf.Key

  var keyMask [TABLE_HEIGHT]bool
  var keyMaskP [TABLE_HEIGHT]bool

  var msgMask BitMatrixRow

  randomVectorKeys(keys[:])
  utils.RandomVector(keyMask[:])

  copy(keyMaskP[:], keyMask[:])
  copy(keysP[:], keys[:])

  keyMaskP[yIdx] = !keyMask[yIdx]

  var err error
  keysP[yIdx], err = prf.NewKey()
  if err != nil {
    return err
  }

  msgMask, err = computeMessageMask(keys[yIdx], keysP[yIdx], msg, xIdx)
  if err != nil {
    return err
  }

  if corrupted {
    log.Printf("Bogus!")
    msgMask[2] = 0xff
    keys[1][1] = 0xff
  }

  for i := 0; i < NUM_SERVERS; i++ {
    var plainQuery InsertQuery

    plainQuery.MessageMask = msgMask
    plainQuery.Keys = keys
    plainQuery.KeyMask = keyMask

    if (i & 1) > 0 {
      plainQuery.Keys = keysP
      plainQuery.KeyMask = keyMaskP
    }

    var err error
    args.Query[i], err = EncryptQuery(i, plainQuery)
    if err != nil {
      log.Fatal("Could not encrypt: ", err)
    }
  }

  return nil
}

func computeMessageMask(key prf.Key, keyP prf.Key,
    msg SlotContents, xIdx int) (BitMatrixRow, error) {

  var msgMask BitMatrixRow
  prfA, err := prf.NewPrf(key)
  if err != nil {
    return msgMask, err
  }

  prfB, err := prf.NewPrf(keyP)
  if err != nil {
    return msgMask, err
  }

  prfA.Evaluate(msgMask[:])
  prfB.Evaluate(msgMask[:])

  msg_row := MessageToRow(msg, xIdx)
  XorRows(&msgMask, &msg_row)

  return msgMask, nil
}

func ComputeProofVector(keys []prf.Key, keyMask []bool) [][]byte {
  vec := make([][]byte, len(keys))

  boolToByte := func(b bool) byte {
    if b {
      return 0x00
    } else {
      return 0xff
    }
  }

  for i:=0; i<len(vec); i++ {
    vec[i] = make([]byte, len(keys[i]) + 1)
    vec[i][0] = boolToByte(keyMask[i])
    copy(vec[i][1:], keys[i][:])
  }

  return vec
}

func computeProof(keys, keysP []prf.Key, keyMask, keyMaskP []bool, differAt int) (schnorr.ManyEvidence, []group.Element, []*big.Int, []group.Element, []*big.Int) {
  vecA := ComputeProofVector(keys, keyMask)
  vecB := ComputeProofVector(keysP, keyMaskP)

  return proof.VectorProve(vecA, vecB, differAt)
}

func randomVectorKeys(lst []prf.Key) error {
  var err error
  for i := 0; i < len(lst); i++ {
    lst[i], err = prf.NewKey()
    if err != nil {
      return err
    }
  }

  return nil
}

func boolToInt(b bool) int64 {
  if (b) {
    return 1
  } else {
    return 0
  }
}

func RandomMessage() (int, int, SlotContents, error) {
  var err error
  var xIdx, yIdx int
  var msg SlotContents

  xIdx, err = utils.RandomInt(TABLE_WIDTH)
  if err != nil {
    return 0, 0, msg, err
  }
  yIdx, err = utils.RandomInt(TABLE_HEIGHT)
  if err != nil {
    return 0, 0, msg, err
  }

  msg, err = RandomSlot()
  return xIdx, yIdx, msg, err
}


