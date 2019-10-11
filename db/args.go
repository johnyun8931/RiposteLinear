package db

import (
	"log"
	"math/big"

	"bitbucket.org/henrycg/riposte/prf"
	"bitbucket.org/henrycg/riposte/utils"
)

var curve = utils.CommonCurve

func InitializeUploadArgs(args *UploadArgs1, msg *Plaintext, corrupted bool) ([]SlotContents, error) {
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
		return nil, err
	}

	msgMask, msgBitShares, err := computeMessageMask(keys[msg.Y], keysP[msg.Y], msg, keyMask[msg.Y])
	if err != nil {
		return nil, err
	}

	/*
		if corrupted {
			log.Printf("Bogus!")
			msgMask[2] = 0xff
			keys[1][1] = 0xff
		}
	*/

	plainQueries := make([]InsertQuery1, 2)
	for i := 0; i < NUM_SERVERS; i++ {
		plainQueries[i].KeyIndex = i
		plainQueries[i].MessageMask = msgMask
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

	return msgBitShares, nil
}

func SetUploadArgs2(msgIntShares []SlotContents,
	upArgs1 *UploadArgs1, upRes1 *UploadReply1) *UploadArgs2 {
	out := new(UploadArgs2)
	copy(out.HashKey[:], upRes1.HashKey[:])
	out.Uuid = upRes1.Uuid

	//log.Printf("msgInt=%v", msgInt)

	// Split hash into shares
	var err error
	var queries [2]InsertQuery2
	mint := new(big.Int)
	for i := 0; i < len(queries); i++ {
		queries[i].MsgShare = SlotToInt(&upRes1.HashKey, msgIntShares[i][:])
		if i == 1 {
			queries[i].MsgShare.Sub(IntModulus, queries[i].MsgShare)
		}
		log.Printf("%v => %v", upRes1.HashKey, queries[i].MsgShare)
		out.Query[i], err = EncryptQuery(i, &queries[i])
		if err != nil {
			panic("Encrypt error")
		}

		mint.Add(mint, queries[i].MsgShare)
	}
	mint.Mod(mint, IntModulus)
	log.Printf("mint: %v", mint)
	return out
}

func SetUploadArgs3(msg *Plaintext,
	upArgs1 *UploadArgs1, upRes1 *UploadReply1,
	upArgs2 *UploadArgs2, upRes2 *UploadReply2) *UploadArgs3 {
	out := new(UploadArgs3)
	copy(out.HashKey[:], upRes1.HashKey[:])
	out.Uuid = upRes1.Uuid

	// Compute test values and proof
	//makeProof()

	var err error
	var queries [2]InsertQuery3
	for i := 0; i < len(queries); i++ {
		//queries[i].MsgShare = shares[i]
		//log.Printf("%v => %v", shares[i])
		out.Query[i], err = EncryptQuery(i, &queries[i])
		if err != nil {
			panic("Encrypt error")
		}
	}

	return out
}

func computeMessageMask(key prf.Key, keyP prf.Key, msg *Plaintext, xorBit bool) (BitMatrixRow,
	[]SlotContents, error) {
	slotShares := make([]SlotContents, 2)

	var msgMask BitMatrixRow
	var prfOut0 BitMatrixRow
	var prfOut1 BitMatrixRow
	prf0, err := prf.NewPrf(key)
	if err != nil {
		return msgMask, slotShares, err
	}

	prf1, err := prf.NewPrf(keyP)
	if err != nil {
		return msgMask, slotShares, err
	}

	prf0.Evaluate(prfOut0[:])
	prf1.Evaluate(prfOut1[:])

	msg_row := MessageToRow(msg)
	XorRows(&msgMask, &prfOut0)
	XorRows(&msgMask, &prfOut1)
	XorRows(&msgMask, &msg_row)

	start := msg.X * SLOT_LENGTH
	end := (msg.X + 1) * SLOT_LENGTH
	if xorBit {
		xorEq(slotShares[0][:], msg.Message[:])
		xorEq(slotShares[0][:], prfOut1[start:end])
		copy(slotShares[1][:], prfOut1[start:end])
	} else {
		xorEq(slotShares[0][:], prfOut0[start:end])
		xorEq(slotShares[1][:], msg.Message[:])
		xorEq(slotShares[1][:], prfOut0[start:end])
	}

	return msgMask, slotShares, nil
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
