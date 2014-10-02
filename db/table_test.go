package db

import (
  "testing"
)

func TestSimple(t *testing.T) {
  tab := new(SlotTable)
  tab.ForeachCell(func(_, _ int, value *SlotContents) {
    for i := 0; i<len(value.Message); i++ {
      value.Message[i] = 2
    }
  })

  if tab.table[0][0].Message[0] != 2 {
    t.Fail()
  }

  tab.Clear()

  if tab.table[0][0].Message[0] != 0 {
    t.Fail()
  }
}

func TestEndToEnd(t *testing.T) {
  xIdx, yIdx, msg, err := RandomMessage()
  if err != nil {
    t.FailNow()
  }

  var args UploadArgs
  err = InitializeUploadArgs(&args, xIdx, yIdx, msg)
  if err != nil {
    t.FailNow()
  }

  // Args has encrypted insert queries
  slotTables := make([]SlotTable, NUM_SERVERS)
  for i := 0; i<NUM_SERVERS; i++ {
    // Decrypt query
    var query *InsertQuery
    query, err = DecryptQuery(i, args.Query[i])
    if err != nil {
      t.FailNow()
    }

    // Add to table
    queries := make([]*InsertQuery, 1)
    queries[0] = query
    slotTables[i].processQuery(queries)
  }

  // Combine tables 
  replies := new([NUM_SERVERS]DumpReply)
  for i := 0; i<NUM_SERVERS; i++ {
    replies[i].Entries = new(BitMatrix)
    slotTables[i].CopyAndClear(replies[i].Entries)
  }

  b := revealCleartext(*replies)

  if b[yIdx][xIdx].Message != msg.Message {
    t.Fail()
  }
}

