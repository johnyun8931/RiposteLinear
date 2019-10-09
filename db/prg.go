package db

import (
	"math/big"

	"bitbucket.org/henrycg/riposte/utils"
)

// A server uses a ReplayPRG to recover the shared values
// that the client sent it (in the form of a PRGHints struct).
type ReplayPRG struct {
	rand *utils.BufPRGReader
	seed utils.PRGKey
	cur  int
}

// Produce a new ReplayPRG object for the given server/leader combo.
func NewReplayPRG() *ReplayPRG {
	out := new(ReplayPRG)
	return out
}

// Import the compressed secret-shared values from hints.
func (p *ReplayPRG) Import(seed utils.PRGKey) {
	p.seed = seed
	p.rand = utils.NewBufPRG(utils.NewPRG(&p.seed))
	p.cur = 0
}

// Recover a secret-shared value that is shared in a field
// that uses modulus mod.
func (p *ReplayPRG) Get(mod *big.Int) *big.Int {
	out := p.rand.RandInt(mod)
	p.cur++

	return out
}

// Split the value secret into two shares modulo mod.
func Share(secret *big.Int) []*big.Int {
	nPieces := 2
	out := make([]*big.Int, nPieces)

	acc := new(big.Int)
	for i := 0; i < nPieces-1; i++ {
		out[i] = utils.RandInt(IntModulus)

		acc.Add(acc, out[i])
	}

	acc.Sub(secret, acc)
	acc.Mod(acc, IntModulus)
	out[nPieces-1] = acc

	return out
}
