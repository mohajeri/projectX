// Databricks notebook source
val inputPath = getArgument("inputPath", "test")
val testJsonData = sqlContext.read.json(inputPath)

// COMMAND ----------

Thread.sleep(60000)
display(testJsonData)

// COMMAND ----------

val outPath = getArgument("outputPath", "test")
testJsonData.write.format("parquet").mode("overwrite").save(outPath)