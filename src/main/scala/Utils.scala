

sealed trait Coin
case object Heads extends Coin
case object Tails extends Coin

sealed trait Operation
case object Increment extends Operation
case object Decrement extends Operation


object Utils {
  type Edge[A] = (A, A)
}
