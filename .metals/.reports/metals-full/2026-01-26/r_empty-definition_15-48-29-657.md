error id: file://<WORKSPACE>/src/main/scala/Musicien.scala:context.
file://<WORKSPACE>/src/main/scala/Musicien.scala
empty definition using pc, found symbol in pc: 
empty definition using semanticdb
empty definition using fallback
non-local guesses:
	 -akka/actor/context.
	 -context.
	 -scala/Predef.context.
offset: 384
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
  val displayActor = context@@.actorOf(Props[DisplayActor], name = "displayActor")

  def receive = {

    // Initialisation
    case Start => {
      displayActor ! Message("Musicien " + this.id + " is created")

      // TODO : Que faire à l'initialisation ?
      // Il faut contacter les autres musiciens s'ils existent (régulièrement jusqu'à 30 secondes)
      // Et sinon, à la fin des 30 secondes, s'arrêter.

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