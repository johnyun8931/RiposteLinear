
package prf

import (
//  "encoding/binary"

  "crypto/aes"
  "crypto/cipher"
  "crypto/rand"
  "errors"
  "math/big"
)

// Length of PRF seed (in bytes)
const KEY_LENGTH = 16

type Key [KEY_LENGTH]byte

type Prf struct {
  block cipher.Block
}

func NewKey() (Key, error) {
  key := new(Key)
  n, err := rand.Read(key[:])
  if n != KEY_LENGTH {
    return *key, errors.New("Invalid key length")
  }
  return *key, err
}

func NewPrf(k Key) (Prf, error) {
  var p Prf
  var err error
  p.block, err = aes.NewCipher(k[:])
  return p, err
}

func (p *Prf) Evaluate(to_encrypt []big.Int, modulus *big.Int, add bool) {
  // IV is all zeros (we will never use
  // this key again)
  iv := make([]byte, aes.BlockSize)

  n_bytes := len(modulus.Bytes())
  cipher_bytes := make([]byte, n_bytes*len(to_encrypt))

  stream := cipher.NewCTR(p.block, iv)
  stream.XORKeyStream(cipher_bytes, cipher_bytes)

  as_int := new(big.Int)
  offset := 0
  for i := 0; i < len(to_encrypt); i++ {
    as_int.SetBytes(cipher_bytes[offset:(offset+n_bytes)])
//    as_int.Mod(as_int, modulus)

    if add {
      to_encrypt[i].Add(&to_encrypt[i], as_int)
    } else {
      to_encrypt[i].Sub(&to_encrypt[i], as_int)
    }
    to_encrypt[i].Mod(&to_encrypt[i], modulus)

    offset += n_bytes
  }
}


