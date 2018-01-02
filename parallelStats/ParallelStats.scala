package parallelStats

import scala.io.Source

class ParallelStats(dataOrFile: AnyRef) {

  lazy val xs: List[Double] = inputData()
  lazy val sortedXs: List[Double] = quickSort(xs)

  def inputData(): List[Double] = {
    dataOrFile match {
      case s: String => Source.fromFile(s).mkString.split(",").par.map(_.toDouble).toList
      case ls: List[Int] => ls.par.map(_.toDouble).toList
      case ls: List[Double] => ls
    }
  }

  def min(): Option[Double] = {
    xs match {
      case List() => None
      case _ => Some(xs.par.foldLeft(xs(0)) { (x, y) => if (x > y) y else x })
    }
  }

  def innerMax(xs: List[Double]): Option[Double] = {
    xs match {
      case List() => None
      case _ => Some(xs.par.foldLeft(xs.head) { (x, y) => if (x < y) y else x })
    }
  }

  def max(): Option[Double] = {
    innerMax(xs)
  }

  def quickSort(xs: List[Double]): List[Double] = {
    /*
    @annotation.tailrec
    def aQuickSort(xs: List[Int], accum1: List[Int], accum2: List[Int]): List[Int] = xs match {
      case List() => List()
      case List(_) => xs
      case a :: tail => aQuickSort(tail.filter(x=>x <= a)) :::
        List(a) ::: aQuickSort(tail.filter(x=>x > a))
    }
    aQuickSort(xs)
    }*/
    xs match {
      case List() => List()
      case List(_) => xs
      case a::tail => quickSort(tail.par.filter(x => x <= a).toList):::List(a):::quickSort(tail.par.filter(x => x > a).toList)
    }
  }


  protected def innerPercentile(xs: List[Double], q: Int): Option[Double] = xs match {
    case List() => None
    case _ => {
      val sortedXs = quickSort(xs)
      val idx: Double = sortedXs.length * q / 100.0
      if (idx == idx.round) {
        Some((sortedXs(idx.toInt - 1) + sortedXs(idx.toInt)) / 2)
      }
      else Some(sortedXs(idx.round.toInt - 1))
    }
  }

  def percentile(q: Int): Option[Double] = {
    innerPercentile(xs, q)
  }

  def median(): Double = {
    innerPercentile(xs, 50).get
  }

  def quartiles(): Option[(Double, Double, Double)] = {
    if (xs.length < 3) {
      return None
    }
    val mid: Int = xs.length / 2
    if (xs.length % 2 == 0) {
      Some((innerPercentile(sortedXs.take(mid), 50).get, innerPercentile(sortedXs, 50).get, innerPercentile(sortedXs.drop(mid), 50).get))
    }
    else {
      Some((innerPercentile(sortedXs.take(mid), 50).get, innerPercentile(sortedXs, 50).get, innerPercentile(sortedXs.drop(mid + 1), 50).get))
    }
  }

  def interquartileRange(): Option[Double] = {
    try {
      val qs = quartiles().get
      return Some(qs._3 - qs._1)
    } catch {
      case e: NoSuchElementException => return None
    }
  }

  // According to Turkey's fences criterion
  def outliers(): List[Double] = {
    try {
      val qs = quartiles().get
      val lower = qs._1 - 1.5 * (qs._3 - qs._1)
      val upper = qs._3 + 1.5 * (qs._3 - qs._1)
      val sortedXs = quickSort(xs).par
      (for (x <- sortedXs; if x < lower || x > upper) yield x).toList
    } catch {
      case e: NoSuchElementException => List()
    }
  }

  def mean(): Option[Double] = {
    xs match {
      case List() => None
      case _ => Some(xs.par.foldLeft(0.0)(_ + _) / xs.length.toDouble)
    }
  }

  def std(): Option[Double] = {
    xs match {
      case List() => None
      case _ => {
        val avg = mean().get
        Some(Math.sqrt(xs.par.map(x => Math.pow(x - avg, 2)).foldLeft(0.0)(_ + _) / xs.length))
      }
    }
  }

  def mode(): Option[List[Double]] = {
    sortedXs match {
      case List() => None
      //case List(v) => Some(v)
      case _ =>
        var indices = List(0)
        var occurCount = List[(Double, Int)]()
        sortedXs.par.fold(sortedXs.head){ (a, b) => {
          if (a != b) {
            indices = indices:::List(sortedXs.indexOf(b))
            sortedXs.indexOf(b)}
          else 0
        }
        }

        indices.par.fold(0){(a, b) => {
          if(a != b) {
            occurCount = occurCount ::: List(sortedXs(a) -> (b - a))
          }
          b
        }
        }
        occurCount = occurCount:::List(sortedXs.last -> (sortedXs.length - occurCount.par.foldLeft(0)((a, b) => a + b._2)))

        val modeCounts = innerMax(occurCount.par.map(x=>x._2.toDouble).toList)

        Some(occurCount.par.filter(_._2 == modeCounts).map(x => x._1).toList)
    }

  }
}
