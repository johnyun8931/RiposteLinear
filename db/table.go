package db

import (
  "henrycg/email/prf"
)

/************
 * Actual DB manipulation
 */

func (t *SlotTable) processRow(row_idx int, queries []*InsertQuery, done chan error) {
//  log.Printf("Processing row %v", row_idx)
  nQueries := len(queries)
  prfs := make([]prf.Prf, nQueries)
  rowBits := make([]bool, nQueries)

  for i := 0; i < nQueries; i++ {
    // For each row, use key to generate PRF output for that row
    var err error
    prfs[i], err = prf.NewPrf(queries[i].Keys[row_idx])
    if err != nil {
      done <-err
    }
    rowBits[i] = queries[i].KeyMask[row_idx]
  }

  var i uint64
  for i = 0; i < uint64(TABLE_WIDTH); i++ {
    for j := 0; j < len(queries); j++ {
      prfs[j].Evaluate(i, t.table[row_idx][i].Message[:])

      // If row bitmask is set, then XOR in the message mask too
      if rowBits[j] {
        t.table[row_idx][i] = AddSlots(t.table[row_idx][i], queries[j].MessageMask[i])
      }
    }
  }

 // log.Printf("Processing row %v -- done", row_idx)
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
  }

  t.tableMutex.Unlock()
  t.debugTable()
  return nil
}

type ForeachFunc func(col int, row int, value *SlotContents)

func (t *SlotTable) ForeachCell(f ForeachFunc) {
  t.tableMutex.Lock()
  for i := 0; i<TABLE_HEIGHT; i++ {
    for j := 0; j<TABLE_WIDTH; j++ {
      f(i, j, &t.table[i][j])
    }
  }
  t.tableMutex.Unlock()
}

func (t *SlotTable) Clear() {
  var empty SlotContents
  t.ForeachCell(func(_ int, _ int, m *SlotContents) {
    *m = empty
  })
}

func (t *SlotTable) CopyAndClear(dest *BitMatrix) {
  var empty SlotContents
  t.ForeachCell(func(i int, j int, m *SlotContents) {
    dest[i][j] = *m
    *m = empty
  })
}


func (t *SlotTable) debugTable() {
  /*
  f := func(data [TABLE_HEIGHT][TABLE_WIDTH]SlotContents) {
    // it in the plaintext table
    for i := 0; i<TABLE_HEIGHT; i++ {
      for j := 0; j<TABLE_WIDTH; j++ {
        fmt.Printf("%v", data[i][j].Message)
      }
      fmt.Printf ("\n")
    }
  }
  fmt.Printf("---- Table ----\n")
  t.tableMutex.Lock()
  f(*t.table)
  t.tableMutex.Unlock()
  */
  return
}

