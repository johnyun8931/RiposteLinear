package db

import (
  "log"

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
      prfs[j].Evaluate(i, t.entries[row_idx][i].Message[:])

      // If row bitmask is set, then XOR in the message mask too
      if rowBits[j] {
        t.entries[row_idx][i] = AddSlots(t.entries[row_idx][i], queries[j].MessageMask[i])
      }
    }
  }

 // log.Printf("Processing row %v -- done", row_idx)
  done<-nil
}

func (t *SlotTable) processQuery(queries []*InsertQuery) error {
  log.Printf("Processing query %d [len %v]", t.ServerIdx, len(queries))
  t.entriesMutex.Lock()

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

  t.ClientsServed += len(queries)
  log.Printf("Clients served: %d", t.ClientsServed)
  t.entriesMutex.Unlock()
  log.Printf("Done processing query %d", t.ServerIdx)
  t.debugTable()
  return nil
}


