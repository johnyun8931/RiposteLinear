package db

import (
  "log"
//  "fmt"
//  "math/big"

  "henrycg/email/prf"
  "henrycg/email/utils"
//  "henrycg/zkp/schnorr"
)

var curve = utils.CommonCurve


func InitializeUploadArgs(args *UploadArgs, xIdx int, yIdx int,
    msg SlotContents) error {

  // Create random values for secret sharing
  var keys [TABLE_HEIGHT]prf.Key
  var keysP [TABLE_HEIGHT]prf.Key

  var keyMask [TABLE_HEIGHT]bool
  var keyMaskP [TABLE_HEIGHT]bool

  var msgMask [TABLE_WIDTH]SlotContents

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

  /*
  for i := 0; i<TABLE_HEIGHT; i++ {
    fmt.Printf("%v\n\t%v\n\t%v\n\t%v\n\t%v\n", i, keyMask[i], keyMaskP[i], keys[i], keysP[i])
  }
  */

  computeMessageMask(msgMask[:], keys[yIdx], keysP[yIdx], msg, xIdx)

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

func computeMessageMask(msgMask []SlotContents,
    key prf.Key, keyP prf.Key,
    msg SlotContents, xIdx int) error {

  prfA, err := prf.NewPrf(key)
  if err != nil {
    return err
  }

  prfB, err := prf.NewPrf(keyP)
  if err != nil {
    return err
  }

  var i uint64
  for i = 0; i < uint64(TABLE_WIDTH); i++ {
    prfA.Evaluate(i, msgMask[i].Message[:])
    prfB.Evaluate(i, msgMask[i].Message[:])
  }

  msgMask[xIdx] = AddSlots(msgMask[xIdx], msg)

  return nil
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

