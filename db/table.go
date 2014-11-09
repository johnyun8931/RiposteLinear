package db

import (
  "log"
  "henrycg/email/prf"
)

/************
 * Actual DB manipulation
 */

func (t *SlotTable) expandRow(allTables []BitMatrix, queries []*InsertQuery, row int, c chan int) {
  for q := 0; q < len(queries); q++ {
    row_prf, err := prf.NewPrf(queries[q].Keys[row])
    if err != nil {
      panic("Can't create PRG!")
    }

    rowBit := queries[q].KeyMask[row]
    row_prf.Evaluate(allTables[q][row][:])

    // XOR row i of query q into the database table
    XorRows(&t.table[row], &allTables[q][row])
    if rowBit {
      // If row bitmask is set, then XOR in the message mask to
      // the table too
      XorRows(&t.table[row], &queries[q].MessageMask)
    }
  }

  c <- 0
}


// XOR all of the rows in src into dst
func xorTable(dst *BitMatrixRow, src *BitMatrix, c chan int) {
  for i := 0; i < TABLE_HEIGHT; i++ {
    XorRows(dst, &src[i])
  }

  c <- 0
}

func (t *SlotTable) processQueries(queries []*InsertQuery) ([]BitMatrixRow, error) {
  t.tableMutex.Lock()

  // For each query, expand seeds to the size of the whole DB table
  allTables := make([]BitMatrix, len(queries))
  allRows := make([]BitMatrixRow, len(queries))

  log.Printf("Making allTables")
  // For each row i and query q, XOR allTables[q][i] into table[i]
  c := make(chan int, len(queries))
  for i := 0; i < TABLE_HEIGHT; i++ {
    go t.expandRow(allTables, queries, i, c)
  }

  for i := 0; i < TABLE_HEIGHT; i++ {
    <-c
  }

  log.Printf("Making allRows")
  // For each query q and row i, XOR allTables[q][i] into allRows[q]
  for q := 0; q < len(queries); q++ {
    go xorTable(&allRows[q], &allTables[q], c)
  }

  for i := 0; i < len(queries); i++ {
    <-c
  }

  t.tableMutex.Unlock()

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

