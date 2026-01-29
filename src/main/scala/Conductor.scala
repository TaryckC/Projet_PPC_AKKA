package upmc.akka.leader
import akka.actor.{Props, Actor, ActorRef, ActorSystem}
import scala.util.Random

object ConductorObject {
  case class StartGame()
  case class ReceiveMeasure(measure: DataBaseActor.Measure)
}

class Conductor(provider: ActorRef, player: ActorRef) extends Actor {
  import ConductorObject._
  import ProviderActor._
  import DataBaseActor._
  import scala.concurrent.duration._
  import context.dispatcher

  def receive = {
    case StartGame() => {
      provider ! getMeasure(diceRoll());
    }
    case ReceiveMeasure(measure) => {
      player ! measure
      context.system.scheduler.scheduleOnce((1800) milliseconds)(
        self ! StartGame()
      )
    }
  }

  def diceRoll(): Int = {
    Random.nextInt(6) + 1 + Random.nextInt(6) + 1
  }
}
