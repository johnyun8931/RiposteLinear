package main

import (
  "log"
//  "fmt"
//  "math/big"

  "henrycg/email/db"
  "henrycg/email/prf"
  "henrycg/email/utils"
//  "henrycg/zkp/schnorr"
)

var curve = utils.CommonCurve

func initializeUploadArgs(args *db.UploadArgs, xIdx int, yIdx int,
    msg db.SlotContents) error {

  // Create random values for secret sharing
  var keys [db.TABLE_HEIGHT]prf.Key
  var keysP [db.TABLE_HEIGHT]prf.Key

  var keyMask [db.TABLE_HEIGHT]bool
  var keyMaskP [db.TABLE_HEIGHT]bool

  var msgMask [db.TABLE_WIDTH]db.SlotContents

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
  for i := 0; i<db.TABLE_HEIGHT; i++ {
    fmt.Printf("%v\n\t%v\n\t%v\n\t%v\n\t%v\n", i, keyMask[i], keyMaskP[i], keys[i], keysP[i])
  }
  */

  computeMessageMask(msgMask[:], keys[yIdx], keysP[yIdx], msg, xIdx)

  for i := 0; i < db.NUM_SERVERS; i++ {
    var plainQuery db.InsertQuery

    plainQuery.MessageMask = msgMask
    plainQuery.Keys = keys
    plainQuery.KeyMask = keyMask

    if (i & 1) > 0 {
      plainQuery.Keys = keysP
      plainQuery.KeyMask = keyMaskP
    }

    var err error
    args.Query[i], err = db.EncryptQuery(i, plainQuery)
    if err != nil {
      log.Fatal("Could not encrypt: ", err)
    }
  }

  return nil
}

func computeMessageMask(msgMask []db.SlotContents,
    key prf.Key, keyP prf.Key,
    msg db.SlotContents, xIdx int) error {

  prfA, err := prf.NewPrf(key)
  if err != nil {
    return err
  }

  prfB, err := prf.NewPrf(keyP)
  if err != nil {
    return err
  }

  var i uint64
  for i = 0; i < uint64(db.TABLE_WIDTH); i++ {
    prfA.Evaluate(i, msgMask[i].Message[:])
    prfB.Evaluate(i, msgMask[i].Message[:])
  }

  msgMask[xIdx] = db.AddSlots(msgMask[xIdx], msg)

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

