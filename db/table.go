package db

import (
	"bitbucket.org/henrycg/riposte/prf"
	"fmt"
	"log"
)

/************
 * Actual DB manipulation
 */

func (t *SlotTable) expandRow(query *InsertQuery, row int) {
	var rowData BitMatrixRow
	row_prf, err := prf.NewPrf(query.Key.Keys[row])
	if err != nil {
		panic("Can't create PRG!")
	}

	rowBit := query.Key.KeyMask[row]
	row_prf.Evaluate(rowData[:])

	// XOR row i of query q into the database table
	t.tableMutex.Lock()
	XorRows(&t.table[row], &rowData)
	if rowBit {
		// If row bitmask is set, then XOR in the message mask to
		// the table too
		XorRows(&t.table[row], &query.Key.MessageMask)
	}
	t.tableMutex.Unlock()
}

// XOR all of the rows in src into dst
func xorTable(dst *BitMatrixRow, src *BitMatrix, c chan int) {
	for i := 0; i < TABLE_HEIGHT; i++ {
		XorRows(dst, &src[i])
	}

	c <- 0
}

func (t *SlotTable) processQuery(query *InsertQuery) {

	// Expand seeds to the size of the whole DB table

	log.Printf("Making allTables")
	// For each row i and query q, XOR allTables[q][i] into table[i]
	for i := 0; i < TABLE_HEIGHT; i++ {
		t.expandRow(query, i)
	}

}

type ForeachFunc func(row int, value *BitMatrixRow)

func (t *SlotTable) ForeachRow(f ForeachFunc) {
	c := make(chan int, TABLE_HEIGHT)
	t.tableMutex.Lock()
	for i := 0; i < TABLE_HEIGHT; i++ {
		go func(j int) {
			f(j, &t.table[j])
			c <- 0
		}(i)
	}

	for i := 0; i < TABLE_HEIGHT; i++ {
		<-c
	}
	t.tableMutex.Unlock()
}

func (t *SlotTable) Clear() {
	var empty BitMatrixRow
	t.ForeachRow(func(_ int, row *BitMatrixRow) {
		*row = empty
	})
}

func (t *SlotTable) CopyToAndClear(dest *BitMatrix) {
	var empty BitMatrixRow
	t.ForeachRow(func(idx int, row *BitMatrixRow) {
		dest[idx] = *row
		*row = empty
	})
}

func (t *SlotTable) Xor(other *BitMatrix) {
	t.ForeachRow(func(idx int, row *BitMatrixRow) {
		XorRows(row, &other[idx])
	})
}

func (t *SlotTable) debugTable() {

	fmt.Printf("---- Table ----\n")
	t.ForeachRow(func(idx int, row *BitMatrixRow) {
		for i := 0; i < len(row); i++ {
			fmt.Printf("%v ", row[i])
		}
		fmt.Printf("\n")
	})

	return
}
