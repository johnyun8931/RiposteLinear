package main

import (
  "crypto/rand"
  "crypto/sha256"
  "math/big"
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
  stX, _, evX := createVectorProof(db.TABLE_WIDTH, xIdx)
  stY, _, evY := createVectorProof(db.TABLE_HEIGHT, yIdx)

  // Create random values for secret sharing
  var randVecsX [db.TABLE_WIDTH]bool
  var randVecsY [db.TABLE_HEIGHT]db.SlotContents

  utils.RandomVector(randVecsX[:])
  randomVectorMsg(randVecsY[:])

  // Compute the differing bit position
  xStar := !randVecsX[xIdx]
  yStar := db.AddSlots(randVecsY[yIdx], msg)

  // Compute commits to X and Y
  var commitX db.CommitRow
  var commitY db.CommitCol

  // Compute g^{bit_i} h^{r_i}
  for i := 0; i<db.TABLE_WIDTH; i++ {
    commitX[i] = stX.GtoXs[i].X
    if randVecsX[i] {
      commitX[i] = curve.Mul(curve.GeneratorG(), commitX[i])
    }
  }

  for i := 0; i<db.TABLE_HEIGHT; i++ {
    commitY[i] = stY.GtoXs[i].X
    m := hashString(randVecsY[i].Message[:])
    commitY[i] = curve.Mul(curve.Pow(curve.GeneratorG(), m), commitY[i])
  }

  for i := 0; i < db.NUM_SERVERS; i++ {
    var plainQuery db.InsertQuery

    copy(plainQuery.XCoords[:], randVecsX[:])
    copy(plainQuery.YCoords[:], randVecsY[:])
    copy(plainQuery.XCommits[:], commitX[:])
    copy(plainQuery.YCommits[:], commitY[:])
    plainQuery.XProof = evX
    plainQuery.YProof = evY

    if (i & 1) == 0 {
      plainQuery.XCoords[xIdx] = xStar
    }

    if (i & 2) == 0 {
      plainQuery.YCoords[yIdx] = yStar
    }

    var err error
    args.Query[i], err = db.EncryptQuery(i, plainQuery)
    if err != nil {
      log.Fatal("Could not encrypt: ", err)
    }
  }

  return nil
}

func createVectorProof(num, bogusIdx int) (
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

func hashString(b []byte) *big.Int {
  h := sha256.Sum224(b)
  return new(big.Int).SetBytes(h[:])
}

