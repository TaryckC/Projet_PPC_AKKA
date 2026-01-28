error id: file://<WORKSPACE>/src/main/scala/Musicien.scala:
file://<WORKSPACE>/src/main/scala/Musicien.scala
empty definition using pc, found symbol in pc: 
empty definition using semanticdb
empty definition using fallback
non-local guesses:
	 -akka/actor.
	 -akka/actor#
	 -akka/actor().
	 -scala/Predef.
	 -scala/Predef#
	 -scala/Predef().
offset: 1001
uri: file://<WORKSPACE>/src/main/scala/Musicien.scala
text:
```scala
package upmc.akka.leader

import akka.actor._

case class Start()

class Musicien(val id: Int, val terminaux: List[Terminal]) extends Actor {

  /*
     Chaque musicien possède :
          - Une liste de contact (ContactList) : Sert à garder à jours qui est vivant, qui est mort, qui est le leader
          -
   */

  // Les differents acteurs du systeme
  val displayActor = context.actorOf(Props[DisplayActor], name = "displayActor")

  def receive = {

    // Initialisation
    case Start => {
      displayActor ! Message("Musicien " + this.id + " is created")

      // TODO : Que faire à l'initialisation ?
      // Il faut contacter les autres musiciens s'ils existent (régulièrement jusqu'à 30 secondes)
      // Et sinon, à la fin des 30 secondes, s'arrêter.

      for (i <- 0 to 3) {
        val host = terminaux(i).ip
        val port = terminaux(i).port
        val path = s"akka.tcp://MozartSystem$i@127.0.0.1:$port/user/Musicien$i"

        context.actorSelection(path).resolveOne(3.s@@econds).onComplete {
          case Success(ref) => ref ! Hello(id, ref)
          case Failure(
                ex
              ) => // TODO : Attention à traiter les exceptions si le problème se présente.
        }
      }

    }

    // Réception d'un message suite à la création d'un musicien
    case Hello(id) => {
      // ContactList : gère les contacts des
    }

  }
}

```


#### Short summary: 

empty definition using pc, found symbol in pc: 