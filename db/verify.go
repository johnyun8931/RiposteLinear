package db

import (
  "log"

  "henrycg/zkp/group"
  "henrycg/zkp/schnorr"
  "henrycg/email/utils"
)

var curve = utils.CommonCurve

func ValidateUpload(serverIdx int, query *InsertQuery) bool {
  // Server 0: X,  Y
  // Server 1: X', Y
  // Server 2: X,  Y'
  // Server 3: X', Y'
  // Recreate proof statement and verify it

  // XXX Should short-circuit these checks for efficiency!
  xComValid := xCommitIsValid(serverIdx, query)
  yComValid := yCommitIsValid(serverIdx, query)
  xValid := xProofIsValid(serverIdx, query)
  yValid := yProofIsValid(serverIdx, query)
  log.Printf("X com is valid? %v", xComValid)
  log.Printf("Y com is valid? %v", xComValid)
  log.Printf("XProof is valid? %v", xValid)
  log.Printf("YProof is valid? %v", yValid)

  return xValid && yValid && xComValid && yComValid
}

func xProofIsValid(serverIdx int, query *InsertQuery) bool {
  var st schnorr.ManyStatement
  st.GtoXs = make([]schnorr.Statement, TABLE_WIDTH)

  invG := curve.Inverse(curve.GeneratorG())
  for i:=0; i<TABLE_WIDTH; i++ {
    st.GtoXs[i].G = curve.GeneratorH()
    if (serverIdx & 1) > 0 {
      // Has X'
      st.GtoXs[i].X = query.XCommits[i]
    } else {
      // Has X
      st.GtoXs[i].X = query.XpCommits[i]
    }

    // If Commit = g^{bit_i} h^{r_i}, divide off 
    // the g term to get h^{r_i}.
    if query.XCoords[i] {
      st.GtoXs[i].X = curve.Mul(st.GtoXs[i].X, invG)
    }
  }

  return schnorr.ManyVerify(curve, st, query.XProof)
}

func yProofIsValid(serverIdx int, query *InsertQuery) bool {
  var st schnorr.ManyStatement
  st.GtoXs = make([]schnorr.Statement, TABLE_HEIGHT)

  for i:=0; i<TABLE_HEIGHT; i++ {
    st.GtoXs[i].G = curve.GeneratorH()
    if (serverIdx & 2) > 0 {
      // Has Y'
      st.GtoXs[i].X = query.YCommits[i]
    } else {
      // Has Y
      st.GtoXs[i].X = query.YpCommits[i]
    }

    // If Commit = g^{m_i} h^{r_i}, divide off 
    // the g term to get h^{r_i}.
    gToM := curve.Pow(curve.GeneratorG(),
      utils.HashString(query.YCoords[i].Message[:]))
    st.GtoXs[i].X = curve.Mul(st.GtoXs[i].X, curve.Inverse(gToM))
  }

  return schnorr.ManyVerify(curve, st, query.YProof)
}

func xCommitIsValid(serverIdx int, query *InsertQuery) bool {
  g := curve.GeneratorG()
  h := curve.GeneratorH()

  var truth []group.Element
  if serverIdx & 1 > 0 {
    // Have X'
    truth = query.XpCommits[:]
  } else {
    // Have X
    truth = query.XCommits[:]
  }

  for i:=0; i<TABLE_WIDTH; i++ {
    com := curve.Pow(h, query.XSecrets[i])
    if query.XCoords[i] {
      com = curve.Mul(g, com)
    }

    if !curve.AreEqual(com, truth[i]) {
      return false
    }
  }

  return true
}

func yCommitIsValid(serverIdx int, query *InsertQuery) bool {
  g := curve.GeneratorG()
  h := curve.GeneratorH()

  var truth []group.Element
  if serverIdx & 2 > 0 {
    // Have Y'
    truth = query.YpCommits[:]
  } else {
    // Have Y
    truth = query.YCommits[:]
  }

  for i:=0; i<TABLE_HEIGHT; i++ {
    com := curve.Pow(h, query.YSecrets[i])
    com = curve.Mul(com, curve.Pow(g, utils.HashString(query.YCoords[i].Message[:])))

    if !curve.AreEqual(com, truth[i]) {
      return false
    }
  }

  return true
}
