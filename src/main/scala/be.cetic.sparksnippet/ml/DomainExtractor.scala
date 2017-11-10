package be.cetic.sparksnippet.ml

import java.net.URL

import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.util.Identifiable

class DomainExtractor(override val uid: String) extends UnaryTransformer[String, String, DomainExtractor]  {

   def this() = this(Identifiable.randomUID("domainExtractor"))

   override protected def createTransformFunc: String => String = (url: String) => scala.util.Try(new URL(url).getHost).toOption.getOrElse("UNKOWN")

   override protected def validateInputType(inputType: org.apache.spark.sql.types.DataType): Unit = {
      require(inputType == org.apache.spark.sql.types.StringType)
   }

   override protected def outputDataType: org.apache.spark.sql.types.DataType = {
      org.apache.spark.sql.types.StringType
   }
}

object Main{
   def main(args: Array[String]): Unit =
   {
      val domainExtractor = new DomainExtractor().
         setInputCol("request").
         setOutputCol("domain")
   }
}

