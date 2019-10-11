package db

import (
	//	"log"

	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"math/big"
	//"bitbucket.org/henrycg/riposte/utils"
)

func proofPrfSetup(key []byte) cipher.Block {
	cipher, err := aes.NewCipher(key)
	if err != nil {
		panic("Cipher error")
	}
	return cipher
}

func proofPrfEval(aes cipher.Block, idx int) *big.Int {
	//size := 16
	out := new(big.Int)
	enc := make([]byte, 16)
	in := make([]byte, 16)

	binary.LittleEndian.PutUint64(in[0:], uint64(idx))
	aes.Encrypt(enc, in)

	out.SetBytes(enc)
	out.Mod(out, IntModulus)
	return out
}

// Set
//   z1 = z1 + (m*r)
//   z2 = z2 + (m*r^2)
// Tmp is a temporary value
func updateTestValues(z1, z2, m, r, tmp *big.Int) {
	// z1 = <m, r_i>
	tmp.Mul(r, m)
	z1.Add(z1, tmp)
	z1.Mod(z1, IntModulus)

	// z2 = <m, r^2_i>
	tmp.Mul(tmp, r)
	z2.Add(z2, tmp)
	z2.Mod(z2, IntModulus)

	//log.Printf("z1=%v, z2=%v, m=%v, r=%v", z1, z2, m, r)
}

/*
func getTestValues(key []byte, msg *big.Int, idx int) (*big.Int, *big.Int) {
	z1 := new(big.Int)
	z2 := new(big.Int)
	tmp := new(big.Int)

	r := proofPrfEval(proofPrfSetup(key), idx)
	updateTestValues(z1, z2, msg, r, tmp)
	return z1, z2
}*/

func updateRowTestValues(row *BitMatrixRow, yIdx int, isServerB bool,
	hashKey *[32]byte, aes cipher.Block, z1 *big.Int, z2 *big.Int, tmp *big.Int) {

	for x := 0; x < TABLE_WIDTH; x++ {
		// Hash contents of row using poly1305
		msg := SlotToInt(hashKey, row[x*SLOT_LENGTH:(x+1)*SLOT_LENGTH])
		if isServerB {
			msg.Sub(IntModulus, msg)
		}

		// Compute sketch values
		idx := xyToInt(x, yIdx)
		//log.Printf("Idx", idx)
		r := proofPrfEval(aes, idx)

		// Update sketch values
		updateTestValues(z1, z2, msg, r, tmp)
	}
}

func getTestValues(hashKey *[32]byte, plain *Plaintext, msgInt, t1, t2 *big.Int) {
	idx := xyToInt(plain.X, plain.Y)
	r := proofPrfEval(proofPrfSetup(hashKey[:]), idx)

	// Set
	//  z1 = <m, r>
	//  z2 = <m, r^2>
	updateTestValues(t1, t2, msgInt, r, new(big.Int))

	// Set
	//  t1 = z1^2
	//  t2 = z2*m

	t1.Mul(t1, t1)
	t1.Mod(t1, IntModulus)

	t2.Mul(t2, msgInt)
	t2.Mod(t2, IntModulus)
}
