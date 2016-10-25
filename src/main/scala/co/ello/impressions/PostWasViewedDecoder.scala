package co.ello.impressions

import org.apache.avro.file.{DataFileReader, SeekableByteArrayInput}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}

import scala.collection.JavaConversions._

object PostWasViewedDecoder {
  def apply(byteArray: Array[Byte]): Seq[Impression] = {

      val datumReader = new GenericDatumReader[GenericRecord]()
      val seekableInput = new SeekableByteArrayInput(byteArray)
      val dataFileReader = new DataFileReader[GenericRecord](seekableInput, datumReader)

      object PostWasViewedRecord {
        def unapply(record : GenericRecord): Option[GenericRecord] =
          if (record.getSchema().getName() == "post_was_viewed") Some(record) else None
      }

      dataFileReader.iterator().toSeq collect { case PostWasViewedRecord(record) => Impression(record.get("post").asInstanceOf[GenericRecord].get("id").toString(),
                                                                                               record.get("author").asInstanceOf[GenericRecord].get("id").toString(),
                                                                                               Option(record.get("viewer").asInstanceOf[GenericRecord]) match {
                                                                                                 case Some(viewer) => viewer.get("id").toString()
                                                                                                 case None => null
      }) }
  }
}
