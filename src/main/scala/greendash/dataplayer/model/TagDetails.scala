package greendash.dataplayer.model

case class TagDetails(tagName: String, measurementType: String, tagId: String, tagType: String, train: String, processBlock: String) {
    def toJson =
        s""" "tagName": "$tagName", "measurementType": "$measurementType", "tagId": "$tagId", "tagType": "$tagType", "train": "$train", "processBlock": "$processBlock" """
}

