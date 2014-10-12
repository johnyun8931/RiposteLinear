
package prf

import (
//  "encoding/binary"

  "crypto/aes"
  "crypto/cipher"
  "crypto/rand"
  "errors"
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

func (p *Prf) Evaluate(to_encrypt []byte) {
  // IV is all zeros (we will never use
  // this key again)
  iv := make([]byte, aes.BlockSize)

  stream := cipher.NewCTR(p.block, iv)
  stream.XORKeyStream(to_encrypt, to_encrypt)
}


