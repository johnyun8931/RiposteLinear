package db

import (
  "log"
  "math/big"

  "henrycg/zkp/group"
  "henrycg/email/proof"
  "henrycg/email/utils"
)

//var curve = utils.CommonCurve

func ValidateUpload(serverIdx int, query *InsertQuery) bool {
  // Check that commitments are valid
  msgVec := ComputeProofVector(query.Keys[:], query.KeyMask[:])
  var myCommits []group.Element
  var otherCommits []group.Element

  if serverIdx == 0 {
    myCommits = query.CommitsA
    otherCommits = query.CommitsB
  } else {
    myCommits = query.CommitsB
    otherCommits = query.CommitsA
  }

  keyCommitValid := CommitIsValid(msgVec, myCommits, query.KeyCommitSecrets)
  keyProofValid := proof.VectorVerify(msgVec[:], query.KeyCommitSecrets, otherCommits, query.KeyProof, serverIdx == 0)

  log.Printf("Key commits are valid? %v", keyCommitValid)
  log.Printf("Key proof is valid? %v", keyProofValid)

  // XXX Should short-circuit these checks for efficiency!
  return keyCommitValid && keyProofValid
}

func CommitIsValid(msgvec [][]byte, commits []group.Element, secrets []big.Int) bool {
  g := curve.GeneratorG()
  h := curve.GeneratorH()

  for i:=0; i<TABLE_HEIGHT; i++ {
    com := curve.Pow(h, &secrets[i])
    com = curve.Mul(com, curve.Pow(g, utils.HashString(msgvec[i][:])))

    if !curve.AreEqual(commits[i], com) {
      return false
    }
  }

  return true
}


/* XXX removing ZKPs for now
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
*/
