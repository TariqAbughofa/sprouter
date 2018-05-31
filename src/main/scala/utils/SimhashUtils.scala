package utils

/**
  * Created by tariq on 09/12/17.
  */


object SimhashUtils {

  implicit class BitOperations(int: Int) {

    def distance(int2: Int) = {
      Integer.bitCount(int ^ int2)
    }

    def toHashString: String = {
      String.format(
        "%32s",
        Integer.toBinaryString(int)
      ).replace(" ", "0")
    }

    def isBitSet(bit: Int): Boolean = {
      ((int >> bit) & 1) == 1
    }
  }

  implicit class simhashMethods(str: String) {

    def shingles = {
      str.replaceAll("\\s+", " ").split(" ").sliding(2).map(_.mkString(" ").hashCode()).toArray
    }

    def simhash = {
      val aggHash = shingles.flatMap({ hash =>
        Range(0, 32).map({ bit =>
          (bit, if(hash.isBitSet(bit)) 1 else -1)
        })
      }).groupBy(_._1).mapValues(_.map(_._2).sum > 0).toArray
      buildSimhash(0, aggHash)
    }

    def weightedSimhash = {
      val features = shingles
      val totalWords = features.length
      val aggHashWeight = features.zipWithIndex.map({case (hash, id) =>
        (hash, 1.0 - id / totalWords.toDouble)
      }).flatMap({ case (hash, weight) =>
        Range(0, 32).map({ bit =>
          (bit, if(hash.isBitSet(bit)) weight else -weight)
        })
      }).groupBy(_._1).mapValues(_.map(_._2).sum > 0).toArray
      buildSimhash(0, aggHashWeight)
    }

    private def buildSimhash(simhash: Int, aggBit: Array[(Int, Boolean)]): Int = {
      if(aggBit.isEmpty) return simhash
      val (bit, isSet) = aggBit.head
      val newSimhash = if(isSet) {
        simhash | (1 << bit)
      } else {
        simhash
      }
      buildSimhash(newSimhash, aggBit.tail)
    }

  }
}
