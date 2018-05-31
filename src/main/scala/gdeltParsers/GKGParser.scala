package gdeltParsers
/**
  * Created by tariq on 05/12/17.
  */

case class GKG(
              recordID: String,
              date: String,
              sourceCollectionIdentifier: Int,
              sourceCommonName: String,
              documentIdentifier: String,
              counts: Array[Count],
              themes: Array[String],
              locations: Array[Location],
              persons: Array[String],
              personsEnhanced: Array[(String, Int)],
              organizations: Array[String],
              organizationsEnhanced: Array[(String, Int)],
              tone: Array[Tone],
              allNames: Array[String]
              )

case class Count(
                countType: String,
                num: Long,
                objectType: String,
                location: Option[Location]
                )

case class Location(
                   locationType: Int,
                   name: String,
                   countryCode: String,
                   adm1Code: String,
                   latitude: Float,
                   longitude: Float,
                   featureID: String
                   )

case class Tone(
               score: Float,
               positiveScore: Float,
               negativeScore: Float,
               polarity: Float,
               activityReferenceDensity: Float,
               selfReferenceDensity: Option[Float],
               wordCount: Option[Float]
               )

object GKGParser {

  private def toCount(countStr:String): Count = {
    val countList = countStr.split("#")
    val location = if (countList.length > 4)
        Some(toLocation(countList.view(3, countList.length).toArray.mkString("#")))
      else
        None
    Count(countList(0), countList(1).toLong, countList(2), location)
  }

  private def toLocation(locationStr: String) : Location = {
    val locationList = locationStr.split("#")
    val latitude = if (locationList(4).nonEmpty) locationList(4).toFloat else 0
    val longitude = if (locationList(5).nonEmpty) locationList(5).toFloat else 0
    Location(locationList(0).toInt, locationList(1), locationList(2),
      locationList(3), latitude, longitude, locationList(6))
  }

  private def toTone(toneStr: String) : Tone = {
      val toneList = toneStr.split("#")
      val srd = if (toneList.length < 6) None else Some(toneList(5).toFloat)
      val wc = if (toneList.length < 7) None else Some(toneList(6).toFloat)
      Tone(toneList(0).toFloat, toneList(1).toFloat, toneList(2).toFloat,
        toneList(3).toFloat, toneList(4).toFloat, srd, wc)
  }

  def toCaseClass(recordRaw: String) : GKG = {
    val record = recordRaw.split("\t", -1)
    val counts = record(5).split(";").filterNot(_.isEmpty).map(toCount)
    val themes = record(7).split(";")
    val locations = record(9).split(";").filterNot(_.isEmpty).map(toLocation)
    val persons = record(11).split(";")
    val persons2 = record(12).split(";").filterNot(_.isEmpty).map(x => {
      val l = x.split(",")
      (l(0), l(1).toInt)
    })
    val organizations = record(13).split(";")
    val organizations2 = record(14).split(";").filterNot(_.isEmpty).map(x => {
      val l = x.split(",")
      (l(0), l(1).toInt)
    })
    val tone = record(16).split(";").filterNot(_.isEmpty).map(toTone)
    val allNames = record(22).split(";").filterNot(_.isEmpty).map(_.split("|").last)
    GKG(record(0), record(1), record(2).toInt, record(3), record(4), counts, themes,
      locations, persons, persons2, organizations, organizations2, tone, allNames)
  }

  def toField(recordRaw: String, offset: Int):String = {
    val record = recordRaw.split("\t")
    if (record.length > offset) record(offset) else ""
  }

  def toPersonsEnhanced(recordRaw: String): Array[(String, Int)] = {
    toField(recordRaw, 12).split(";").filterNot(_.isEmpty).map(x => {
      val l = x.split(",")
      (l(0), l(1).toInt)
    })
  }

}
