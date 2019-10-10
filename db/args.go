package db

import (
	"log"
	"math/big"

	"bitbucket.org/henrycg/riposte/prf"
	"bitbucket.org/henrycg/riposte/utils"
)

var curve = utils.CommonCurve

func InitializeUploadArgs(args *UploadArgs1, msg *Plaintext, corrupted bool) error {

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

	keyMaskP[msg.Y] = !keyMask[msg.Y]

	var err error
	keysP[msg.Y], err = prf.NewKey()
	if err != nil {
		return err
	}

	msgMask, err = computeMessageMask(keys[msg.Y], keysP[msg.Y], msg)
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
		plainQueries[i].KeyIndex = i
		plainQueries[i].MessageMask = msgMask
		//plainQueries[i].Key.MessageShare = shares[i]
		plainQueries[i].Keys = keys
		plainQueries[i].KeyMask = keyMask

		if (i & 1) > 0 {
			plainQueries[i].Keys = keysP
			plainQueries[i].KeyMask = keyMaskP
		}
	}

	for i := 0; i < NUM_SERVERS; i++ {
		var err error
		args.Query[i], err = EncryptQuery(i, &plainQueries[i])
		if err != nil {
			log.Fatal("Could not encrypt: ", err)
		}
	}
	return nil
}

func SetUploadArgs2(msg *Plaintext, upArgs1 *UploadArgs1, upRes1 *UploadReply1) *UploadArgs2 {
	out := new(UploadArgs2)
	copy(out.HashKey[:], upRes1.HashKey[:])
	out.Uuid = upRes1.Uuid

	// Hash message using hash fn specified by HashKey
	_, shares := computeMessageShares(&out.HashKey, &msg.Message)

	// Split hash into shares
	var err error
	var queries [2]InsertQuery2
	for i := 0; i < len(queries); i++ {
		queries[i].MsgShare = shares[i]
		log.Printf("%v => %v", shares[i])
		out.Query[i], err = EncryptQuery(i, &queries[i])
		if err != nil {
			panic("Encrypt error")
		}
	}

	return out
}

func computeMessageShares(hashKey *[32]byte, msg *SlotContents) (*big.Int, []*big.Int) {
	h := SlotToInt(hashKey, msg)
	return h, Share(h)
}

func computeMessageMask(key prf.Key, keyP prf.Key, msg *Plaintext) (BitMatrixRow, error) {

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

	msg_row := MessageToRow(msg)
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

func RandomMessage() (*Plaintext, error) {
	out := new(Plaintext)
	var err error

	out.X = utils.RandIntShort(TABLE_WIDTH)
	out.Y = utils.RandIntShort(TABLE_HEIGHT)

	err = RandomSlot(&out.Message)
	return out, err
}
