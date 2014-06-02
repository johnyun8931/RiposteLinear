package db

import (
  "log"

  "henrycg/zkp/schnorr"
  "henrycg/email/utils"
)

var curve = utils.CommonCurve

func (t *SlotTable) validateUpload(query InsertQuery) bool {
  // Server 0: X,  Y
  // Server 1: X', Y
  // Server 2: X,  Y'
  // Server 3: X', Y'
  // Recreate proof statement and verify it

  xValid := xProofIsValid(t, query)
  yValid := yProofIsValid(t, query)
  log.Printf("XProof is valid? %v", xValid)
  log.Printf("YProof is valid? %v", yValid)

  // XXX bogus for now
  return true
}

func xProofIsValid(t *SlotTable, query InsertQuery) bool {
  var stX schnorr.ManyStatement
  stX.GtoXs = make([]schnorr.Statement, TABLE_WIDTH)

  // XXX TODO: make sure that commitment vector is valid
  // -- i.e., verify the opening of the commitment C(X)
  // matches the X that the client sent

  invG := curve.Inverse(curve.GeneratorG())
  for i:=0; i<TABLE_WIDTH; i++ {
    stX.GtoXs[i].G = curve.GeneratorH()
    if (t.ServerIdx & 1) == 0 {
      stX.GtoXs[i].X = query.XpCommits[i]
    } else {
      stX.GtoXs[i].X = query.XCommits[i]
    }

    // If Commit = g^{bit_i} h^{r_i}, divide off 
    // the g term to get h^{r_i}.
    if query.XCoords[i] {
      stX.GtoXs[i].X = curve.Mul(stX.GtoXs[i].X, invG)
    }
  }

  return schnorr.ManyVerify(curve, stX, query.XProof)
}

func yProofIsValid(t *SlotTable, query InsertQuery) bool {
  var stY schnorr.ManyStatement
  stY.GtoXs = make([]schnorr.Statement, TABLE_HEIGHT)

  // XXX TODO: make sure that commitment vector is valid
  // -- i.e., verify the opening of the commitment C(X)
  // matches the X that the client sent

  for i:=0; i<TABLE_HEIGHT; i++ {
    stY.GtoXs[i].G = curve.GeneratorH()
    if (t.ServerIdx & 3) == 0 {
      stY.GtoXs[i].X = query.YpCommits[i]
    } else {
      stY.GtoXs[i].X = query.YCommits[i]
    }

    // If Commit = g^{m_i} h^{r_i}, divide off 
    // the g term to get h^{r_i}.
    gToM := curve.Pow(curve.GeneratorG(),
      utils.HashString(query.YCoords[i].Message[:]))
    stY.GtoXs[i].X = curve.Mul(stY.GtoXs[i].X, curve.Inverse(gToM))
  }

  return schnorr.ManyVerify(curve, stY, query.YProof)
}
