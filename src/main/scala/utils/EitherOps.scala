package utils

import cats.implicits.toBifunctorOps

object EitherOps {
  implicit class EitherOps[E, A](private val either: Either[E, A]) {
    def tapError(f: E => Unit): Either[E, A] = either.leftMap { e =>
      f(e); e
    }

    def tap(f: E => Unit,g: A => Unit): Either[E, A] = either.bimap(e => {
      f(e)
      e
    }, a => {
      g(a)
      a
    })
  }
}