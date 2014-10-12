package db

import (
//  "log"
  "math"
  "testing"
)

func closeTo(a, b float64) bool {
  return math.Abs(a - b) < 0.0001
}

func TestBirthday(t *testing.T) {

  if !closeTo(ExpectedCollisions(0, 3), 0) {
    t.Fail()
  }

  if !closeTo(ExpectedCollisions(1, 5), 0) {
    t.Fail()
  }

  if !closeTo(ExpectedCollisions(2, 2), 0.5) {
    t.Fail()
  }

  if !closeTo(ExpectedCollisions(3, 2), 1.0) {
    t.Fail()
  }

  //log.Printf("%v", CollisionProbability(25001, 200000, 25000))
}
