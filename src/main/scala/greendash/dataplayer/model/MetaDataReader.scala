package greendash.dataplayer.model

import com.typesafe.config.ConfigFactory

import scala.collection.mutable

object MetaDataReader {

    val config = ConfigFactory.load()
    val tagsFile = config.getString("tags.file")

    val tagsMap = mutable.Map.empty[String, TagDetails]

    val bufferedSource = io.Source.fromFile(tagsFile)
    for (line <- bufferedSource.getLines) {
        val Array(tagName, measurementType, tagId, tagType, train, processBlock) = line.split(",").map(_.trim).map(_.replaceAll("\"", ""))
        tagsMap(tagName) = TagDetails(tagName, measurementType, tagId, tagType, train, processBlock)
    }
    bufferedSource.close
}


