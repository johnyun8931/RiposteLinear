package db

func AddSlots(a, b SlotContents) SlotContents {
  var res SlotContents
  for i := 0; i < len(a.Message); i++ {
    res.Message[i] = a.Message[i] ^ b.Message[i]
  }
  return res
}

