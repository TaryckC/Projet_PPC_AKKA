class ConductorObject {
  case object ThrowDice
}

class Conductor(val id: Int, val terminaux: List[Terminal]) extends Actor {}
