package greendash.dataplayer.model

case class TagDetails(tagName: String,
                      measurementType: String,
                      tagId: String,
                      equipmentArea: String,
                      instrument: String,
                      tagType: String,
                      train: String,
                      processBlock: String) {
    def toJson =
        s"""
           |"tagName": "$tagName",
           |"measurementType": "$measurementType",
           |"tagId": "$tagId",
           |"equipmentArea": "$equipmentArea",
           |"instrument": "$instrument",
           |"tagType": "$tagType",
           |"train": "$train",
           |"processBlock": "$processBlock"
           |""".stripMargin.replaceAll("\n", " ")
}

