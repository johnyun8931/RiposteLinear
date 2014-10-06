package proof

import (
  "bytes"
  "math/big"

  "henrycg/email/utils"
  "henrycg/zkp/group"
  "henrycg/zkp/schnorr"
)

var curve = utils.CommonCurve

// The two vectors are the same except that:
//    vectorA[differAt] != vectorB[differAt]
func VectorProve(vectorA, vectorB [][]byte, differAt int) (schnorr.ManyEvidence, []group.Element, []*big.Int, []group.Element, []*big.Int) {
  length := len(vectorA)

  // Compute h^{r_i} for i = 1,...,num
  stA, secA := createCommitVector(length, differAt)
  stB, secB := createCommitVector(length, differAt)

  if bytes.Equal(vectorA[differAt], vectorB[differAt]) {
    panic("Invalid args")
  }

  var wit schnorr.ManyWitness
  wit.Xs = make([]schnorr.Witness, length)
  wit.BogusIdx = differAt

  // The secret exponents in the proof are r_i - r'_i
  for i:=0; i<length; i++ {
    wit.Xs[i].X = new(big.Int)
    wit.Xs[i].X.Sub(secA[i], secB[i])
    wit.Xs[i].X.Mod(wit.Xs[i].X, curve.Order())
  }


  // Compute commitments 
  //  C_i = g^{Hash(m_i)} h^{r_i} 
  //    and
  //  C'_i = g^{Hash(m'_i)} h^{r'_i}

  commitA := make([]group.Element, length)
  commitB := make([]group.Element, length)

  for i := 0; i<length; i++ {
    commitA[i] = stA.GtoXs[i].X
    commitB[i] = stB.GtoXs[i].X

    mA := utils.HashString(vectorA[i][:])
    gToMsgA := curve.PowG(mA)

    var gToMsgB group.Element
    if i == differAt {
      mB := utils.HashString(vectorB[i][:])
      gToMsgB = curve.PowG(mB)
    } else {
      gToMsgB = gToMsgA
    }

    commitA[i] = curve.Mul(gToMsgA, commitA[i])
    commitB[i] = curve.Mul(gToMsgB, commitB[i])
  }

  var st schnorr.ManyStatement
  st.GtoXs = make([]schnorr.Statement, length)
  for i := 0; i<length; i++ {
    st.GtoXs[i].G = curve.GeneratorH()
    st.GtoXs[i].X = curve.Mul(commitA[i], curve.Inverse(commitB[i]))
  }

  // Prove
  ev := schnorr.ManyProve(curve, st, wit)

  return ev, commitA, secA, commitB, secB
}

func createCommitVector(num, bogusIdx int) (
  schnorr.ManyStatement, []*big.Int) {
  var st schnorr.ManyStatement
  secrets := make([]*big.Int, num)

  st.GtoXs = make([]schnorr.Statement, num)
  for i:= 0; i<num; i++ {
    secrets[i] = curve.RandomExponent()
    st.GtoXs[i].G = curve.GeneratorH()
    st.GtoXs[i].X = curve.Pow(st.GtoXs[i].G, secrets[i])
  }

  return st, secrets
}

func VectorVerify(vector [][]byte, myCommitSecrets []*big.Int, otherCommits []group.Element, ev schnorr.ManyEvidence, isServerA bool) bool {
  num := len(vector)

  // Recreate the proof statement.

  var st schnorr.ManyStatement
  st.GtoXs = make([]schnorr.Statement, num)
  for i:= 0; i<num; i++ {
    // We have m and r, so recreate commit as C_i = g^{Hash(m_i)} h^{r_i}
    msg := utils.HashString(vector[i][:])
    gToMsg := curve.Pow(curve.GeneratorG(), msg)
    hToR := curve.Pow(curve.GeneratorH(), myCommitSecrets[i])

    st.GtoXs[i].G = curve.GeneratorH()
    st.GtoXs[i].X = curve.Mul(gToMsg, hToR)

    // We want to compute (CommitA / CommitB).
    // If we are server A, subtract commitB, otherwise
    // subtract from commit A.
    if (isServerA) {
      st.GtoXs[i].X = curve.Mul(st.GtoXs[i].X, curve.Inverse(otherCommits[i]))
    } else {
      st.GtoXs[i].X = curve.Mul(otherCommits[i], curve.Inverse(st.GtoXs[i].X))
    }
  }

  return schnorr.ManyVerify(curve, st, ev)
}




