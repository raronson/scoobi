package com.nicta.scoobi
package impl
package io

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

class HdfsDataIterator[A](getReader: (Path, Configuration) => HdfsReader[A], path: Path, c: Configuration) extends Iterator[A] {

  def fs = FileSystem.get(path.toUri, c)

  private var initialised = false
  def init {
    if (!initialised)  {
      remainingReaders = Helper.getFileStatus(path)(c) flatMap { (stat: FileStatus) =>
        val reader = getReader(stat.getPath, c)
        if(reader.hasNext) Some(reader) else None
      }
      initialised = true
    }
  }
  private var remainingReaders: Stream[HdfsReader[A]] = Stream()

  def next(): A = {
    init
    remainingReaders match {
      case cur #:: rest => val n = cur.next(); if(!cur.hasNext) moveNextReader(); n
    }
  }

  def hasNext(): Boolean = {
    init
    remainingReaders match {
      case Stream.Empty         => false
      case cur #:: Stream.Empty => cur.hasNext
      case cur #:: rest         => cur.hasNext || { moveNextReader(); rest.head.hasNext }
    }
  }

  private def moveNextReader() {
    remainingReaders match {
      case cur #:: rest => cur.close(); remainingReaders = rest
      case _            =>
    }
  }

  def close {
    Option(remainingReaders).map(rs => rs.foreach(_.close))
  }
}

trait HdfsReader[A] extends Iterator[A] {
  def iterator(): Iterator[A]
  def close(): Unit
  def hasNext(): Boolean = iterator.hasNext
  def next(): A = iterator.next()
}
