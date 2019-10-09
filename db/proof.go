package db

import (
	"log"

	"crypto/aes"
	"crypto/sha256"
	"encoding/binary"
	"math/big"

	"bitbucket.org/henrycg/riposte/utils"
)

func prfEval(key []byte, idx int) *big.Int {
	//size := 16
	out := new(big.Int)
	cipher, _ := aes.NewCipher(key)
	enc := make([]byte, 32)
	in := make([]byte, 16)

	binary.LittleEndian.PutUint64(in[0:], uint64(idx))
	cipher.Encrypt(enc, in)

	binary.LittleEndian.PutUint64(in[8:], 1)
	cipher.Encrypt(enc[16:], in)

	out.SetBytes(enc)
	out.Mod(out, IntModulus)
	return out
}

func getTestValues(key []byte, msg *big.Int, idx int) (*big.Int, *big.Int) {
	r := prfEval(key, idx)

	// z1 = <m, r_i>
	z1 := new(big.Int)
	z1.Mul(r, msg)
	z1.Mod(z1, IntModulus)

	// z2 = <m, r^2_i>
	z2 := new(big.Int)
	z2.Mul(r, r)
	z2.Mod(z2, IntModulus)
	z2.Mul(msg, z2)
	z2.Mod(z2, IntModulus)

	return z1, z2
}

func getTestValueShares(key []byte, msg *big.Int) (*big.Int, *big.Int) {
	maxLen := xyToInt(TABLE_WIDTH-1, TABLE_HEIGHT-1)

	t1share := new(big.Int)
	t2share := new(big.Int)

	for i := 0; i < maxLen; i++ {
		h_i = hashSlot(

		// Compute [z1] = <[x_i], [r_i]>
		
		// Compute [z2] = <[x_i], [r^2_i]>

	}

	return t1share, t2share
}

func makeProof(chal [sha256.Size]byte, msg *big.Int, idx int) []CorProof {
	out := make([]CorProof, 2)
	var seed utils.PRGKey
	copy(seed[:], chal[0:16])

	prg := NewReplayPRG()
	prg.Import(seed)

	prfKey := chal[16:]
	z1, z2 := getTestValues(prfKey, msg, idx)

	// Sanity test
	//   Should be that z1^2 = m . z_2
	t1 := new(big.Int)
	t1.Mul(z1, z1)
	t1.Mod(t1, IntModulus)

	t2 := new(big.Int)
	t2.Mul(msg, z2)
	t2.Mod(t2, IntModulus)

	if t1.Cmp(t2) != 0 {
		panic("fail")
	}

	log.Printf("t1 %v", t1)
	log.Printf("t2 %v", t2)

	return out
}
