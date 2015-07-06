package db

import (
  //"log"
  "henrycg/riposte/prf"
)

func NewBitMatrix() *BitMatrix {
  return new(BitMatrix)
}

func NewSlotTable() *SlotTable {
  t := new(SlotTable)
  t.table = *NewBitMatrix()
  return t
}

/************
 * Actual DB manipulation
 */

func (t *SlotTable) processRow(is_server_a bool, row_idx int, queries []*InsertQuery, done chan error) {
  //log.Printf("Processing row %v", row_idx)
  nQueries := len(queries)
  for q := 0; q < nQueries; q++ {
    // For each row, use key to generate PRF output for that row
    var err error
    var row_prf prf.Prf
    row_prf, err = prf.NewPrf(queries[q].Keys[row_idx])
    if err != nil {
      done <-err
    }

    rowBit := queries[q].KeyMask[row_idx]
    row_prf.Evaluate(t.table[row_idx][:])

    // If row bitmask is set, then XOR in the message mask too
    if rowBit {
      XorRows(&t.table[row_idx], &queries[q].MessageMask)
    }
  }

  //log.Printf("Processing row %v -- done", row_idx)
  done<-nil
}

func (t *SlotTable) processQuery(is_server_a bool, queries []*InsertQuery) error {
  t.tableMutex.Lock()

  c := make(chan error, TABLE_HEIGHT)
  for i := 0; i < TABLE_HEIGHT; i++ {
    go t.processRow(is_server_a, i, queries, c)
  }

  // Wait for all workers to complete
  for i := 0; i < TABLE_HEIGHT; i++ {
    err := <-c
    if err != nil {
      return err
    }
  }

  t.tableMutex.Unlock()
  t.debugTable()
  return nil
}

type ForeachFunc func(row int, value *BitMatrixRow)

func (t *SlotTable) ForeachRow(f ForeachFunc) {
  t.tableMutex.Lock()
  for i := 0; i<TABLE_HEIGHT; i++ {
    f(i, &t.table[i])
  }
  t.tableMutex.Unlock()
}

func (t *SlotTable) Clear() {
  t.ForeachRow(func(_ int, row *BitMatrixRow) {
    for i := 0; i<len(*row); i++ {
      row[i] = 0x00
    }
  })
}

func (t *SlotTable) CopyAndClear(dest *BitMatrix) {
  t.ForeachRow(func(i int, row *BitMatrixRow) {
    for j := 0; j<len(*row); j++ {
      dest[i][j] = row[j]
      row[j] = 0x00
    }
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

