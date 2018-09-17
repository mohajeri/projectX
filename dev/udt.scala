// Databricks notebook source
// Simple example demonstrating the treatment of a case class with a
// byte array within another case class.

case class IPAddress(bytes: Array[Byte]) {
  override def toString: String = s"IPAddress(Array(${bytes.mkString(", ")}))"
}

// COMMAND ----------

val a = IPAddress(Array(1,2,3,4))
val b = IPAddress(Array(5,6,7,8))
val c = IPAddress(Array(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16))
val d = IPAddress(Array(17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32))

// COMMAND ----------

val x = Array(a, b, c, d)
val xs = sc.parallelize(x).toDS

// COMMAND ----------

xs.show

// COMMAND ----------

case class Rec(a: IPAddress, b: IPAddress) {
  override def toString: String = s"Rec($a, $b)"
}

// COMMAND ----------

val e = Rec(a, b)
val f = Rec(c, d)
val y = Array(e, f)
val ys = sc.parallelize(y).toDS

// COMMAND ----------

ys.show