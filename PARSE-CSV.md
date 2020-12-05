## PARSE CSV

starting with the following code : 

```
val data="""STATION,STATION_NAME,ELEVATION,LATITUDE,LONGITUDE,DATE,HPCP,Measurement Flag,Quality Flag
  COOP:310301,ASHEVILLE NC US,682.1,35.5954,-82.5568,20100101 00:00,99999,],
  COOP:310301,ASHEVILLE NC US,682.1,35.5954,-82.5568,20100101 01:00,0,g,
  COOP:310301,ASHEVILLE NC US,682.1,35.5954,-82.5568,20100102 06:00,1, ,
"""
```

Provide an implementation of the function : 

```scala
def convertToDataset(csvData: String): Dataset[PrecipitationSample] = ???
```

where PrecipitationSample2 have the following definition:

```scala
case class PrecipitationSample(
    Station: String, 
    StationName: String, 
    Elevation: Double, 
    Latitude: Double, 
    Longitude: Double, 
    Date: String, 
    Hpcp: Int, 
    MeasurementFlag: String, 
    QualityFlag: String
)
```

## SOLUTION

```scala
def headerToPascalCase(h: String) = h.split("[ _]+").map(_.toLowerCase.capitalize).mkString("")
```

```scala
def toTuple9(values: Array[String]) = values match {
  case Array(a,b,c,d,e,f,g,h,i) => (a,b,c,d,e,f,g,h,i)
  case Array(a,b,c,d,e,f,g,h) =>  (a,b,c,d,e,f,g,h, "")  // because "a,".split(",") returns Array(a) instead of Array(a, "")
  case _ => ("","","","","","","","","") // if not recognized, empty values
}
```

```scala
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
```



```scala
def normalizeHeaders(df: DataFrame, headers: String): DataFrame = {
  val fieldNames = df.schema.fieldNames
  val h = headers.split(",").toList
  var tempDF = df
  (0 to fieldNames.size - 1).foreach { i => 
    // update DataFrame
    val currentField = fieldNames(i)
    val currentHeader = h(i)
    val updatedFieldName = headerToPascalCase(currentHeader)
    // f : (DataFrame, String) => DataFrame
    tempDF = tempDF.withColumnRenamed(currentField, updatedFieldName )
  }
  tempDF
}
```

```scala
def convertToDataSet(csvData: String): Dataset[PrecipitationSample] = {
  val headers :: rows = data.split("\n").toList
  var df = rows.map(_.split(",")).map(toTuple9).toDF
  df = normalizeHeaders(df, headers)
  df = df.withColumn("Elevation",col("Elevation").cast(DoubleType))
  df = df.withColumn("Latitude",col("Latitude").cast(DoubleType))
  df = df.withColumn("Longitude",col("Longitude").cast(DoubleType))
  df = df.withColumn("Hpcp", col("Hpcp").cast(IntegerType))
              
  return df.as[PrecipitationSample]
}
```

```scala
val ds = convertToDataSet(data)
ds.show
ds.printSchema
```






