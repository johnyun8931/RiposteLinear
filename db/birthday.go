package db

import "math"

// When we have the specified number of balls and
// bins and throw the balls into the bins independently
// and uniformly at random, how many bins contain two
// or more balls?
func ExpectedCollisions(balls, bins int) float64 {
  m := float64(balls)
  n := float64(bins)

  // Let  m = # balls 
  //      n = # bins

  // Pr[Bin i has > 1 ball] = 
  //      1 - Pr[Bin i has 1 ball] - Pr[Bin i has 0 balls]

  // Pr[Bin i has 0 balls] = (1 - 1/n)^m

  // The probability that bin i gets 1 ball is the probability
  // that ball j ends up in bin i and all balls != j end up 
  // in some other bin. This probability is summed over all balls j.
  // 
  // Pr[Bin i has 1 ball] = m*(1/n)(1 - 1/n)^{m-1}

  // Pr[Bin i has at least 2 balls] 
  //    = 1 - (1 - 1/n)^m - m*(1/n)(1 - 1/n)^{m-1}
  //    = 1 - (1 - 1/n)^{m-1} [(1 - 1/n) + (m/n)]
  //    = 1 - (1 - 1/n)^{m-1} [(n + m - 1)/n]
  //    = 1 - (n-1)^{m-1} (n + m - 1) n^{-m}

  // The expected number of bins with 2 or more balls
  // is just this value summed n times (over the bins).

  // E[# bins with 2 or more balls] = n*Pr[Bin i has at least 2 balls]
  //    = n - (n-1)^{m-1} (n + m - 1) n^{-m+1}

  // Call the latter term X. To avoid underflow, calculate
  // log X:
  //
  // log X = (m-1) log(n-1) + log(n+m-1) + (1-m) log(n)

  log_X := (m - 1) * math.Log(n - 1)
  log_X += math.Log(n + m - 1)
  log_X += (1 - m) * math.Log(n)

  X := math.Exp(log_X)
  return n - X
}

// Using Markov's inequality, calculate an upper
// that we see the given number of collisions in 
// so many balls and bins.
func CollisionProbability(balls, bins, coll_observed int) float64 {
  if coll_observed > bins {
    panic("Cannot have more collisions than balls or bins")
  }

  if coll_observed > balls {
    return 0.0
  }

  // Markov's inequality is:
  //   Pr[X >= a] <= E[X]/a

  // Letting X be the number of bins with collisions,
  // we can say that:
  //   Pr[(# collisions) > (collisions seen)] <= E[# collisions]/coll_observed
  coll := float64(coll_observed)
  return ExpectedCollisions(balls, bins) / coll
}



