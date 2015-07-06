package db

import (
  "bytes"
  "crypto/rand"
  "errors"
  "encoding/gob"

  "code.google.com/p/go.crypto/nacl/box"
  "henrycg/riposte/utils"
)

// XXX This is a terrible way to implement this functionality.
// Box provides authentication, which we don't need. I'm using 
// it now just because I don't want to use Go's PGP or RSA 
// implementations. 
func EncryptQuery(serverIdx int, query InsertQuery) (EncryptedInsertQuery, error) {
  var out EncryptedInsertQuery
  serverPublicKey := utils.ServerBoxPublicKeys[serverIdx]
  var nonce [24]byte
  _, err := rand.Read(nonce[:])
  if err != nil {
    return out, err
  }

  var buf bytes.Buffer
  enc := gob.NewEncoder(&buf)
  err = enc.Encode(query)
  if err != nil {
    return out, err
  }

  myPublicKey, myPrivateKey, err := box.GenerateKey(rand.Reader)

  out.SenderPublicKey = *myPublicKey
  out.Nonce = nonce
  out.Ciphertext = box.Seal(nil, buf.Bytes(), &nonce, serverPublicKey, myPrivateKey)

  /*
  log.Printf("pk   %v", out.SenderPublicKey)
  log.Printf("nc   %v", out.Nonce)
  log.Printf("ct   %v", out.Ciphertext)
  */

  return out, nil
}

func DecryptQuery(serverIdx int, enc EncryptedInsertQuery) (*InsertQuery, error) {
  serverPrivateKey := utils.ServerBoxPrivateKeys[serverIdx]

  /*
  log.Printf("pk   %v", enc.SenderPublicKey)
  log.Printf("nc   %v", enc.Nonce)
  log.Printf("ct   %v", enc.Ciphertext)
  */

  var buf []byte
  buf, okay := box.Open(nil, enc.Ciphertext, &enc.Nonce,
      &enc.SenderPublicKey, serverPrivateKey)

  query := new(InsertQuery)
  if !okay {
    return query, errors.New("Could not decrypt")
  }

  dec := gob.NewDecoder(bytes.NewBuffer(buf))
  err := dec.Decode(&query)
  if err != nil {
    return query, err
  }

  return query, nil

}

func EncryptSlot(serverIdx int, slot []byte) ([]byte, error) {
  serverPublicKey := utils.ServerBoxPublicKeys[serverIdx]
  var nonce [24]byte

  // XXX TODO nonce should increment with time epoch
  myPublicKey, myPrivateKey, err := box.GenerateKey(rand.Reader)
  if err != nil {
    return nil, err
  }

  out := make([]byte, BOX_PUBLIC_KEY_LEN + BOX_OVERHEAD + len(slot))
  copy(out[:], myPublicKey[:])
  buf := box.Seal(nil, slot, &nonce, serverPublicKey, myPrivateKey)
  copy(out[len(myPublicKey):], buf)
  return out, nil
}

func DecryptSlot(serverIdx int, slot []byte) ([]byte, error) {
  // First 32 bytes are the sender's ephemeral public key, 
  // the rest is the message
  serverPrivateKey := utils.ServerBoxPrivateKeys[serverIdx]

  // XXX THIS IS COMPLETELY INSECURE! NONCE SHOULD INCREMENT WITH
  // EACH TIME EPOCH TO PREVENT REPLAY ATTACKS.
  var nonce [24]byte
  var pubkey [32]byte

  copy(pubkey[:], slot[0:32])
  buf, okay := box.Open(nil, slot[32:], &nonce, &pubkey, serverPrivateKey)
  if okay {
    return buf, nil
  } else {
    return nil, errors.New("Decryption failure")
  }
}
