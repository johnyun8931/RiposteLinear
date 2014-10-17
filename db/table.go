package db

import (
//  "log"

  "henrycg/email/prf"
)

/************
 * Actual DB manipulation
 */

func (t *SlotTable) processQuery(query *InsertQuery) (*BitMatrixRow, error) {
  // This contains the sum of all the generated rows, which is used later
  // to make the audit request.
  allRows := new(BitMatrixRow)

  // This holds the output of the current run of the PRF
  var genRow BitMatrixRow

  for i := 0; i < TABLE_HEIGHT; i++ {
    row_prf, err := prf.NewPrf(query.Keys[i])
    if err != nil {
      return allRows, err
    }

    rowBit := query.KeyMask[i]
    row_prf.Evaluate(genRow[:])


    // XOR into the row that holds all generated strings
    XorRows(allRows, &genRow)

    // XOR into the database table
    t.tableMutex.Lock()
    XorRows(&t.table[i], &genRow)
    if rowBit {
      // If row bitmask is set, then XOR in the message mask to
      // the table too
      XorRows(&t.table[i], &query.MessageMask)
    }
    t.tableMutex.Unlock()
  }

  t.debugTable()
  return allRows, nil
}

type ForeachFunc func(row int, value *BitMatrixRow)

func (t *SlotTable) ForeachRow(f ForeachFunc) {
  c := make(chan int, TABLE_HEIGHT)
  t.tableMutex.Lock()
  for i := 0; i<TABLE_HEIGHT; i++ {
    go func(j int) {
      f(j, &t.table[j])
      c <- 0
    }(i)
  }

  for i := 0; i<TABLE_HEIGHT; i++ {
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

func (t *SlotTable) CopyAndClear(dest *BitMatrix) {
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
  /*
  fmt.Printf("---- Table ----\n")
  t.ForeachRow(func(idx int, row *BitMatrixRow) {
    for i := 0; i<len(row); i++ {
      fmt.Printf("%v ", row[i])
    }
    fmt.Printf ("\n")
  })
  */
  return
}

