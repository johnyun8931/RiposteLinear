package db

import (
  "log"
  "testing"
)

func TestSimple(t *testing.T) {
  tab := NewSlotTable()
  tab.ForeachRow(func(_ int, value *BitMatrixRow) {
    for i := 0; i<len(value); i++ {
      value[i] = 0x02
    }
  })

  if tab.table[0][0] != 0x02 {
    t.Fail()
  }

  tab.Clear()

  if tab.table[0][0] != 0x00 {
    t.Fail()
  }
}

func TestEndToEndOnce(t *testing.T) {
  xIdx, yIdx, msg, err := RandomMessage()
  if err != nil {
    t.FailNow()
  }

  var args UploadArgs
  err = InitializeUploadArgs(&args, xIdx, yIdx, msg)
  if err != nil {
    t.FailNow()
  }
  //fmt.Printf("(x,y) = (%v, %v)\n", xIdx, yIdx)
  //for i := 0; i < len(msg); i++ {
  //  fmt.Printf("msg[%v] = (%v)\n", i, &msg[i])
  //}

  // Args has encrypted insert queries
  slotTables := make([]*SlotTable, NUM_SERVERS)
  for i := 0; i<NUM_SERVERS; i++ {
    slotTables[i] = NewSlotTable()

    // Decrypt query
    var query *InsertQuery
    query, err = DecryptQuery(i, args.Query[i])
    if err != nil {
      t.FailNow()
    }

    // Add to table
    queries := make([]*InsertQuery, 1)
    queries[0] = query
    slotTables[i].processQuery(i == 0, queries)
  }

  // Combine tables 
  replies := new([NUM_SERVERS]DumpReply)
  for i := 0; i<NUM_SERVERS; i++ {
    replies[i].Entries = NewBitMatrix()
    slotTables[i].CopyAndClear(replies[i].Entries)
  }

  b := revealCleartext(*replies)
  for i:=0; i<len(b); i++ {
    for j:=0; j<len(b[i]); j++ {
      //fmt.Printf("%v ", &b[i][j])
    }
    //fmt.Printf("\n")
  }

  var out [SLOT_LENGTH]byte
  start := SLOT_LENGTH * xIdx
  for i := 0; i<len(out); i++ {
    out[i] = b[yIdx][start + i]
    if out[i] != msg[i] {
      t.Fatal("Message mismatch", &out[i], &msg[i])
    }
  }
}

func BenchmarkTable(b *testing.B) {
  xIdx, yIdx, msg, err := RandomMessage()
  if err != nil {
    b.FailNow()
  }

  var args UploadArgs
  err = InitializeUploadArgs(&args, xIdx, yIdx, msg)
  if err != nil {
    b.FailNow()
  }

  // Decrypt query
  var query *InsertQuery
  query, err = DecryptQuery(0, args.Query[0])
  if err != nil {
    b.FailNow()
  }

  // Add to table
  queries := make([]*InsertQuery, b.N)
  for i := 0; i < b.N; i++ {
    queries[i] = query

  }

  // Args has encrypted insert queries
  slotTable := NewSlotTable()
  b.ResetTimer()
  slotTable.processQuery(false, queries)
  log.Printf("Here!")
}

