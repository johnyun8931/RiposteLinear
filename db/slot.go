package db

import (
	"crypto/rand"
	"crypto/sha256"
	"math/big"
	"reflect"
	"unsafe"
)

func MessageToRow(msg SlotContents, xIdx int) BitMatrixRow {
	var res BitMatrixRow
	start := SLOT_LENGTH * xIdx
	copy(res[start:], msg[:])
	return res
}

func XorRows(dest, add *BitMatrixRow) {
	xorEq(dest[:], add[:])
}

func RandomSlot() (SlotContents, error) {
	var msg SlotContents
	_, err := rand.Read(msg[:])
	return msg, err
}

func HashSlot(slot SlotContents) [sha256.Size]byte {
	return sha256.Sum256(slot[:])
}

func SlotToInt(slot SlotContents) *big.Int {
	h := HashSlot(slot)
	out := new(big.Int)
	out.SetBytes(h[:])
	return out
}

/* Copied from
 * https://groups.google.com/forum/#!topic/golang-nuts/aPjvemV4F0U
 */

func xoreq64(a, b []uint64) {
	for i := range a {
		a[i] ^= b[i]
	}
}

// touint64 assumes len(x)%8 == 0
func touint64(x []byte) []uint64 {
	xx := make([]uint64, 0, 0)
	hdrp := (*reflect.SliceHeader)(unsafe.Pointer(&xx))
	hdrp.Data = (*reflect.SliceHeader)(unsafe.Pointer(&x)).Data
	hdrp.Len = len(x) / 8
	hdrp.Cap = len(x) / 8
	return xx
}

func xorEq(a, b []byte) {
	if len(a) != len(b) || len(a)%8 != 0 {
		panic("lengths not equal or not a multiple of 8")
	}

	xoreq64(touint64(a), touint64(b))
}

func tableToInts() {

}
