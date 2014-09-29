
package prf

import (
  "crypto/aes"
  "crypto/cipher"
  "crypto/rand"
  "encoding/binary"
  "errors"
)

// Length of PRF seed (in bytes)
const KEY_LENGTH = 32

type Key [KEY_LENGTH]byte

type Block [aes.BlockSize]byte

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
  p := new(Prf)
  var err error
  p.block, err = aes.NewCipher(k[:])
  return *p, err
}

func (p *Prf) Evaluate(i uint64, j uint64) Block {
  //Encode val into 16-byte buffer
  plaintext := make([]byte, aes.BlockSize)
  binary.PutUvarint(plaintext, i)
  binary.PutUvarint(plaintext[8:], j)

  // Evaluate AES using ECB mode on the integer
  ciphertext := make([]byte, len(plaintext))
  p.block.Encrypt(ciphertext[:], plaintext[:])

  out := new(Block)
  copy(out[:], ciphertext[:])
  return *out
}

