package db

import (
  "log"

  "henrycg/email/prf"
)

/************
 * Actual DB manipulation
 */

func (t *SlotTable) processQuery(queries []*InsertQuery) error {
  log.Printf("Processing query %d", t.ServerIdx)
  t.entriesMutex.Lock()

  var err error
  nQueries := len(queries)
  for i := 0; i < TABLE_HEIGHT; i++ {
    prfs := make([]prf.Prf, nQueries)
    rowBits := make([]bool, nQueries)

    for j := 0; j < nQueries; j++ {
      // For each row, use key to generate PRF output for that row
      prfs[j], err = prf.NewPrf(queries[j].Keys[i])
      if err != nil {
        return err
      }
      rowBits[j] = queries[j].KeyMask[i]
    }

    var j uint64
    for j = 0; j < uint64(TABLE_WIDTH); j++ {
      for k := 0; k < len(queries); k++ {
        prfOutput := EvaluatePrf(prfs[k], j)
        t.entries[i][j] = AddSlots(t.entries[i][j], prfOutput)

        // If row bitmask is set, then XOR in the message mask too
        if rowBits[k] {
          t.entries[i][j] = AddSlots(t.entries[i][j], queries[k].MessageMask[j])
        }
      }
    }
  }

  t.entriesMutex.Unlock()
  log.Printf("Done processing query %d", t.ServerIdx)
  t.debugTable()
  return nil
}


