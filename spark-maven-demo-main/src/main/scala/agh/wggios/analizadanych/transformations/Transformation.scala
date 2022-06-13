package agh.wggios.analizadanych.transformations

import agh.wggios.analizadanych.caseclass.Flights

class Transformation(value:Int) {
  val compare_value: Int =value
  def airport_filtering(flight: Flights): Boolean = {
    flight.count > compare_value;
  }
}