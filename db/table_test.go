package db

import (
  "log"
  "math/big"
  "testing"
)

func TestSimple(t *testing.T) {
  tab := NewSlotTable()
  two := big.NewInt(2)
  tab.ForeachRow(func(_ int, value *BitMatrixRow) {
    for i := 0; i<len(value); i++ {
      value[i] = *two
    }
  })

  if two.Cmp(&tab.table[0][0]) != 0 {
    t.Fail()
  }

  tab.Clear()

  zero := big.NewInt(0)
  if zero.Cmp(&tab.table[0][0]) != 0 {
    t.Fail()
  }
}

func TestEndToEndNoProof(t *testing.T) {
  testEndToEndOnce(t, false)
}

func TestEndToEndProof(t *testing.T) {
  testEndToEndOnce(t, true)
}

func testEndToEndOnce(t *testing.T, doProof bool) {
  xIdx, yIdx, msg, err := RandomMessage()
  if err != nil {
    t.FailNow()
  }

  var args UploadArgs
  err = InitializeUploadArgs(&args, xIdx, yIdx, msg, doProof)
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

  var out [SLOT_LENGTH]big.Int
  start := SLOT_LENGTH * xIdx
  for i := 0; i<len(out); i++ {
    out[i] = b[yIdx][start + i]
    if out[i].Cmp(&msg[i]) != 0 {
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
  err = InitializeUploadArgs(&args, xIdx, yIdx, msg, false)
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

