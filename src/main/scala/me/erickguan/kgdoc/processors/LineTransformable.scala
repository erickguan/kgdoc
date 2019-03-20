package me.erickguan.kgdoc.processors

trait LineTransformable {
  def toLine(separator: Char = '\t'): String
}
