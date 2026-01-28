error id: file://<WORKSPACE>/src/main/scala/Musicien.scala:actorOf.
file://<WORKSPACE>/src/main/scala/Musicien.scala
empty definition using pc, found symbol in pc: 
empty definition using semanticdb
empty definition using fallback
non-local guesses:
	 -akka/actor/context/actorOf.
	 -akka/actor/context/actorOf#
	 -akka/actor/context/actorOf().
	 -ContactListActor.context.actorOf.
	 -ContactListActor.context.actorOf#
	 -ContactListActor.context.actorOf().
	 -context/actorOf.
	 -context/actorOf#
	 -context/actorOf().
	 -scala/Predef.context.actorOf.
	 -scala/Predef.context.actorOf#
	 -scala/Predef.context.actorOf().
offset: 539
uri: file://<WORKSPACE>/src/main/scala/Musicien.scala
text:
```scala
package upmc.akka.leader

import akka.actor._

case class Start()

class Musicien(val id: Int, val terminaux: List[Terminal]) extends Actor {

  import ContactListActor._

  // Instanciation des acteurs
  val myContactList: ActorRef =
    context.actorOf(Props[ContactListActor], "contactList")

  /*
     Chaque musicien possède :
          - Une liste de contact (ContactList) : Sert à garder à jours qui est vivant, qui est mort, qui est le leader
          -
   */

  // Les differents acteurs du systeme
  val displayActor = context.a@@ctorOf(Props[DisplayActor], name = "displayActor")

  def receive = {

    // Initialisation
    case Start => {
      displayActor ! Message("Musicien " + this.id + " is created")

      // TODO : Que faire à l'initialisation ?
      // Il faut créer la contactlist
      // Et compteur le nombre de message envoyé

      

      // Il faut contacter les autres musiciens s'ils existent (régulièrement jusqu'à 30 secondes)
      // Et sinon, à la fin des 30 secondes, s'arrêter.

      for (i <- 0 to 3) {
        val host = terminaux(i).ip
        val port = terminaux(i).port
        val path = s"akka.tcp://MozartSystem$i@127.0.0.1:$port/user/Musicien$i"

        context.actorSelection(path).resolveOne(3.seconds).onComplete {
          case Success(ref) =>
            ref ! Hello(id, System.currentTimeMillis()) // timestamp est un long
          case Failure(
                ex
              ) => // TODO : Acteur absent, rien à faire
        }
      }

    }

    // Réception d'un message suite à la création d'un musicien (le musicien se présente)
    case Hello(senderId, timeStamp) => {
      // On transmet les infos à la liste de contacts pour update

    }

    // Après Avoir reçu un Hello, on répond avec Hey et on se présente aussi.
    // TODO : on peut aussi transmettre un timestamp qui correspond à notre date de naissance. (Pour établir la priorité des chefs)
    case Hey(myId, senderId, timeStamp) => {}

  }
}

```


#### Short summary: 

empty definition using pc, found symbol in pc: 