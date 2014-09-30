
package prf

import (
  "crypto/rand"
  "encoding/binary"
  "errors"

  "github.com/tang0th/go-chacha20"
)

// Length of PRF seed (in bytes)
const KEY_LENGTH = 32

const BLOCK_SIZE = 160

type Key [KEY_LENGTH]byte
type Block [BLOCK_SIZE]byte

type Prf struct {
  Nonce []byte
  Key []byte
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
  p.Key = k[:]
  p.Nonce = []byte{0,0,0,0,0,0,0,0}
  return *p, err
}

func (p *Prf) Evaluate(i uint64, j uint64) Block {
  //Encode val into 16-byte buffer
  plaintext := make([]byte, BLOCK_SIZE)
  binary.PutUvarint(plaintext, i)
  binary.PutUvarint(plaintext[8:], j)

  // Evaluate PRF on the integer
  ciphertext := make([]byte, len(plaintext))
  chacha20.XORKeyStream8(ciphertext, plaintext, p.Nonce, p.Key)

  out := new(Block)
  copy(out[:], ciphertext[:])
  return *out
}

