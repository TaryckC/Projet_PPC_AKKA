package upmc.akka.leader
import akka.actor.{Actor, ActorRef}
import scala.util.Random

object ConductorActor {
  case object StartGame
  case class ReceiveMeasure(measure: DataBaseActor.Measure)
  case class UpdateMusicians(musicianIds: List[Int])
}

class Conductor(provider: ActorRef, terminaux: List[Terminal], ownerId: Int)
    extends Actor {
  import ConductorActor._
  import ProviderActor._
  import DataBaseActor._
  import scala.concurrent.duration._
  import context.dispatcher

  var availableMusicians: List[Int] = List()

  def receive = {
    case StartGame => {
      if (availableMusicians.nonEmpty) {
        provider ! getMeasure(diceRoll())
      } else {
        context.system.scheduler.scheduleOnce(1800.milliseconds)(
          self ! StartGame
        )
      }
    }
    case ReceiveMeasure(measure) => {
      // foreach ici parce que ça nous permet d'ignorer si on a reçu None et d'exécuter l'action sinon
      pickMusician(availableMusicians).foreach { targetId =>
        // On communique directement avant avec le musicien qui doit jouer (plus simple que de remonter les appels)
        context
          .actorSelection(musicianPath(targetId)) ! Musicien.PlayMeasure(
          measure
        )
      }
      // On recommene régulièrement
      context.system.scheduler.scheduleOnce(1800.milliseconds)(
        self ! StartGame
      )
    }

    // On peut peut-être préférer directement stocker la références des musicens disponible
    // Mais du coup il faudrait les échanger lors de la poigné de main ("Hello“, "Hey")
    case UpdateMusicians(musicianIds) => {
      availableMusicians = musicianIds.filterNot(_ == ownerId)
    }
  }

  def diceRoll(): Int = {
    Random.nextInt(6) + 1 + Random.nextInt(6) + 1
  }

  // Permet simplement de plus facilement de récupérer le chemin akka pour le musicien
  private def musicianPath(id: Int): String = {
    val port = terminaux(id).port
    s"akka.tcp://MozartSystem$id@127.0.0.1:$port/user/Musicien$id"
  }

  // Choisis un musicien au hasrd (s'il en existe) (Option c'est comme un Maybe en haskell)
  private def pickMusician(musicians: List[Int]): Option[Int] = {
    if (musicians.isEmpty) {
      None
    } else {
      Some(musicians(Random.nextInt(musicians.length)))
    }
  }
}
