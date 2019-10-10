package db

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"log"
	"math/big"

	"bitbucket.org/henrycg/riposte/prf"
	"bitbucket.org/henrycg/riposte/utils"
)

var curve = utils.CommonCurve

func InitializeUploadArgs(args *UploadArgs1, xIdx int, yIdx int,
	msg SlotContents, corrupted bool) error {

	// Create random values for secret sharing
	var keys [TABLE_HEIGHT]prf.Key
	var keysP [TABLE_HEIGHT]prf.Key

	var keyMask [TABLE_HEIGHT]bool
	var keyMaskP [TABLE_HEIGHT]bool

	var msgMask BitMatrixRow

	randomVectorKeys(keys[:])
	utils.RandVectorBool(keyMask[:])

	copy(keyMaskP[:], keyMask[:])
	copy(keysP[:], keys[:])

	keyMaskP[yIdx] = !keyMask[yIdx]

	var err error
	keysP[yIdx], err = prf.NewKey()
	if err != nil {
		return err
	}

	//msgInt, shares := computeMessageShares(msg)
	msgMask, err = computeMessageMask(keys[yIdx], keysP[yIdx], msg, xIdx)
	if err != nil {
		return err
	}

	if corrupted {
		log.Printf("Bogus!")
		msgMask[2] = 0xff
		keys[1][1] = 0xff
	}

	plainQueries := make([]InsertQuery1, 2)
	for i := 0; i < NUM_SERVERS; i++ {
		plainQueries[i].Key.KeyIndex = i
		plainQueries[i].Key.MessageMask = msgMask
		//plainQueries[i].Key.MessageShare = shares[i]
		plainQueries[i].Key.Keys = keys
		plainQueries[i].Key.KeyMask = keyMask

		if (i & 1) > 0 {
			plainQueries[i].Key.Keys = keysP
			plainQueries[i].Key.KeyMask = keyMaskP
		}
	}

	var chal [sha256.Size]byte
	for i := 0; i < NUM_SERVERS; i++ {
		// Get Fiat-Shamir challenge
		h := hashDpfKey(&plainQueries[i].Key)
		xorEq(chal[:], h[:])
	}

	log.Printf("Final challenge: %v", chal)

	//proofs := makeProof(chal, msgInt, xyToInt(xIdx, yIdx))

	// Compute proof
	/*
		for i := 0; i < NUM_SERVERS; i++ {
			plainQueries[i].Proof = proofs[i]
		}
	*/

	for i := 0; i < NUM_SERVERS; i++ {
		var err error
		args.Query[i], err = EncryptQuery1(i, &plainQueries[i])
		h := hashDpfKey(&plainQueries[i].Key)
		log.Printf("hash: %v", h)
		if err != nil {
			log.Fatal("Could not encrypt: ", err)
		}
	}
	return nil
}

func computeMessageShares(msg SlotContents) (*big.Int, []*big.Int) {
	h := SlotToInt(msg)
	return h, Share(h)
}

func computeMessageMask(key prf.Key, keyP prf.Key,
	msg SlotContents, xIdx int) (BitMatrixRow, error) {

	var msgMask BitMatrixRow
	prfA, err := prf.NewPrf(key)
	if err != nil {
		return msgMask, err
	}

	prfB, err := prf.NewPrf(keyP)
	if err != nil {
		return msgMask, err
	}

	prfA.Evaluate(msgMask[:])
	prfB.Evaluate(msgMask[:])

	msg_row := MessageToRow(msg, xIdx)
	XorRows(&msgMask, &msg_row)

	return msgMask, nil
}

func ComputeProofVector(keys []prf.Key, keyMask []bool) [][]byte {
	vec := make([][]byte, len(keys))

	boolToByte := func(b bool) byte {
		if b {
			return 0x00
		} else {
			return 0xff
		}
	}

	for i := 0; i < len(vec); i++ {
		vec[i] = make([]byte, len(keys[i])+1)
		vec[i][0] = boolToByte(keyMask[i])
		copy(vec[i][1:], keys[i][:])
	}

	return vec
}

func randomVectorKeys(lst []prf.Key) error {
	var err error
	for i := 0; i < len(lst); i++ {
		lst[i], err = prf.NewKey()
		if err != nil {
			return err
		}
	}

	return nil
}

func boolToInt(b bool) int64 {
	if b {
		return 1
	} else {
		return 0
	}
}

func xyToInt(xIdx, yIdx int) int {
	return xIdx*TABLE_HEIGHT + yIdx
}

func RandomMessage() (int, int, SlotContents, error) {
	var err error
	var xIdx, yIdx int
	var msg SlotContents

	xIdx = utils.RandIntShort(TABLE_WIDTH)
	yIdx = utils.RandIntShort(TABLE_HEIGHT)

	msg, err = RandomSlot()
	return xIdx, yIdx, msg, err
}

func hashDpfKey(key *DPFKey) [sha256.Size]byte {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	err := enc.Encode(key)
	if err != nil {
		panic("Gob error!")
	}

	return sha256.Sum256(buf.Bytes())
}
