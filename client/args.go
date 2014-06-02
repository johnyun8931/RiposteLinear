package main

import (
  "crypto/rand"
  "log"

  "henrycg/email/db"
  "henrycg/email/utils"
  "henrycg/zkp/schnorr"
)

var curve = utils.CommonCurve

func initializeUploadArgs(args *db.UploadArgs, xIdx int, yIdx int,
    msg db.SlotContents) error {

  // Get blinding values and proofs for X and Y coords
  // Each element of these vectors is h^{r_i} for 
  // randomness r_i.
  stX, _, evX := createCommitVector(db.TABLE_WIDTH, xIdx)
  stXp, _, evXp := createCommitVector(db.TABLE_WIDTH, xIdx)
  stY, _, evY := createCommitVector(db.TABLE_HEIGHT, yIdx)
  stYp, _, evYp := createCommitVector(db.TABLE_HEIGHT, yIdx)

  // Create random values for secret sharing
  var randVecsX [db.TABLE_WIDTH]bool
  var randVecsXp [db.TABLE_WIDTH]bool
  var randVecsY [db.TABLE_HEIGHT]db.SlotContents
  var randVecsYp [db.TABLE_HEIGHT]db.SlotContents

  utils.RandomVector(randVecsX[:])
  randomVectorMsg(randVecsY[:])

  // XXX this is not as space-efficient as it could be...
  copy(randVecsXp[:], randVecsX[:])
  copy(randVecsYp[:], randVecsY[:])

  // Compute the differing bit position

  xStar := !randVecsX[xIdx]
  yStar := db.AddSlots(msg, randVecsY[yIdx])
  randVecsXp[xIdx] = xStar
  randVecsYp[yIdx] = yStar

  // Compute commits to X and Y
  var commitX db.CommitRow
  var commitXp db.CommitRow
  var commitY db.CommitCol
  var commitYp db.CommitCol

  // Set the bogus commit to be:
  //    g^{bit_i} h^{r_i} / g^{bit'_i}

  // Compute g^{bit_i} h^{r_i}
  for i := 0; i<db.TABLE_WIDTH; i++ {
    commitX[i] = stX.GtoXs[i].X
    commitXp[i] = stXp.GtoXs[i].X

    if randVecsX[i] {
      commitX[i] = curve.Mul(curve.GeneratorG(), commitX[i])
    }

    if randVecsXp[i] {
      commitXp[i] = curve.Mul(curve.GeneratorG(), commitXp[i])
    }
  }

  for i := 0; i<db.TABLE_HEIGHT; i++ {
    commitY[i] = stY.GtoXs[i].X
    commitYp[i] = stYp.GtoXs[i].X

    m := utils.HashString(randVecsY[i].Message[:])
    commitY[i] = curve.Mul(curve.Pow(curve.GeneratorG(), m), commitY[i])

    mp := utils.HashString(randVecsYp[i].Message[:])
    commitYp[i] = curve.Mul(curve.Pow(curve.GeneratorG(), mp), commitYp[i])
  }

  for i := 0; i < db.NUM_SERVERS; i++ {
    var plainQuery db.InsertQuery

    plainQuery.XCoords = randVecsX
    plainQuery.YCoords = randVecsY

    plainQuery.XCommits = commitX
    plainQuery.XpCommits = commitXp
    plainQuery.YCommits = commitY
    plainQuery.YpCommits = commitYp

    plainQuery.XProof = evX
    plainQuery.YProof = evY

    if (i & 1) == 0 {
      plainQuery.XCoords = randVecsXp
      plainQuery.XProof = evXp
    }

    if (i & 2) == 0 {
      plainQuery.YCoords = randVecsYp
      plainQuery.YProof = evYp
    }

    var err error
    args.Query[i], err = db.EncryptQuery(i, plainQuery)
    if err != nil {
      log.Fatal("Could not encrypt: ", err)
    }
  }

  return nil
}

func createCommitVector(num, bogusIdx int) (
  schnorr.ManyStatement, schnorr.ManyWitness, schnorr.ManyEvidence) {
  var st schnorr.ManyStatement
  var wit schnorr.ManyWitness

  st.GtoXs = make([]schnorr.Statement, num)
  wit.Xs = make([]schnorr.Witness, num)
  wit.BogusIdx = bogusIdx

  for i:= 0; i<num; i++ {
    wit.Xs[i].X = curve.RandomExponent()
    st.GtoXs[i].G = curve.GeneratorH()
    st.GtoXs[i].X = curve.Pow(st.GtoXs[i].G, wit.Xs[i].X)
  }

//  st.GtoXs[bogusIdx].X = bogusValue

  ev := schnorr.ManyProve(curve, st, wit)
  return st, wit, ev
}


func randomVectorMsg(lst []db.SlotContents) error {
  var err error
  for i := 0; i < len(lst); i++ {
    _, err = rand.Read(lst[i].Message[:])
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

