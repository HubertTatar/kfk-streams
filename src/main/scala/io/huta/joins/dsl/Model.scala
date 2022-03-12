package io.huta.joins.dsl

case class Stream1Data(key: Int, status: String)
case class Stream2Data(key: Int, status: String)
case class StreamJoined(key1: Int, key2: Int, statusA: String, statusB: String)
