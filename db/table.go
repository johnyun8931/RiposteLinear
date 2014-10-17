package db

import (
//  "log"

  "henrycg/email/prf"
)

/************
 * Actual DB manipulation
 */

func (t *SlotTable) processRow(row_idx int, queries []*InsertQuery, done chan error) {
//  log.Printf("Processing row %v", row_idx)
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

func (t *SlotTable) processQuery(queries []*InsertQuery) error {
  t.tableMutex.Lock()

  c := make(chan error, TABLE_HEIGHT)
  for i := 0; i < TABLE_HEIGHT; i++ {
    go t.processRow(i, queries, c)
  }

  // Wait for all workers to complete
  for i := 0; i < TABLE_HEIGHT; i++ {
    err := <-c
    if err != nil {
      return err
    }
    //log.Printf("Done processing %v/%v", i, TABLE_HEIGHT)
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

