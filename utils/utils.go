package utils

import (
  "crypto/rand"
  "math/big"
)

func AllServers() []string {
  return []string {
    "localhost:8080",
    "localhost:8081",
    "localhost:8082",
    "localhost:8083",
    /*
    "localhost:8084",
    "localhost:8085",
    "localhost:8086",
    "localhost:8087",
    */
  }
}

func NumServers() int {
  return len(AllServers())
}

func RandomInt64(max int64) (int64, error) {
  var bigMax *big.Int = big.NewInt(int64(max))
  var out *big.Int
  var err error
  out, err = rand.Int(rand.Reader, bigMax)
  if err != nil {
    return 0, err
  }

  return out.Int64(), nil
}

func RandomInt(max int) (int, error) {
  num, err := RandomInt64(int64(max))
  return int(num), err
}

func RandomVector(lst []bool) error {
  for i := 0; i < len(lst); i++ {
    bit, err := RandomInt(2)
    if err != nil {
      return err
    }
    lst[i] = (bit != 0)
  }

  return nil
}

