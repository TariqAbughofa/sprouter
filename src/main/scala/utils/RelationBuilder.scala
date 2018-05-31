package utils

/**
  * Created by tariq on 24/12/17.
  */



object RelationBuilder {

  // In an average sentence there are 10-20 words, 75-100 characters.
  // In an average paragraph 100-200 words. Which can be averaged into 750-1000 character.
  def buildTuples(sortedEntities: List[(String,Int)], distance: Int = 1000, result: List[(String,String)]=List.empty): List[(String,String)] = {
    if (sortedEntities.isEmpty || sortedEntities.tail.isEmpty) {
      return result
    }
    buildTuples(sortedEntities.tail, distance, getInRange(sortedEntities.head, sortedEntities.tail, distance, result))
  }

  private def getOrderedTuple(tuple: (String, String)): (String, String) = {
    if (tuple._1 < tuple._2) {
      tuple
    } else {
      tuple.swap
    }
  }

  private def getInRange(entity: (String, Int), array: List[(String,Int)], distance:Int, result: List[(String,String)]): List[(String,String)] = {
    if (array.isEmpty || array.head._2 - entity._2 > distance) {
      return result
    }
    // Skipping self-relations - The list can have the same entity with difference positions.
    if (array.head._1 == entity._1) {
      getInRange(entity, array.tail, distance, result)
    } else {
      // Avoiding duplicates - Since we have undirected relations, we should keep some order.
      val newTuple = getOrderedTuple(entity._1, array.head._1)
      getInRange(entity, array.tail, distance, newTuple :: result)
    }
  }
}
