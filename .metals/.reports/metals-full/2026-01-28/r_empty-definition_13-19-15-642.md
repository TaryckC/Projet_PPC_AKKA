error id: file://<WORKSPACE>/src/main/scala/ContactList.scala:Instant.
file://<WORKSPACE>/src/main/scala/ContactList.scala
empty definition using pc, found symbol in pc: 
empty definition using semanticdb
empty definition using fallback
non-local guesses:

offset: 1555
uri: file://<WORKSPACE>/src/main/scala/ContactList.scala
text:
```scala
package upmc.akka.leader

import akka.actor._

import akka.actor.ActorRef
import java.time.Instant

case class Contact(
  id: Int,
  birthDate: Instant,
  lastSeen: Instant,
  ref: ActorRef,
  ticker: Cancellable
)

object ContactListActor {
  case class AddContact(id: Int, timeStamp: String, contactRef: ActorRef)
  case class RemoveContact(id: Int)
  case object GetContacts
}

class ContactListActor extends Actor {

  /*
    
    La liste de contacte va se charger de communiquer avec les listes de contacts des autres musiciens.
    Elle maintient à jours une liste permettant de savoir qui est encore en vie

    Elle communique à son propriétaire la liste des id valide pour la communication

   */

  import ContactListActor._
  import Contact

  var contacts = Map[Int, Contact]()

  def receive: Receive = {
    case AddContact(targetId, timeStamp, contactRef) => {
      val now = Instant.now()
      contacts += targetId -> Contact(
        targetId, 
        now, 
        now, 
        contactRef, 
        context.system.scheduler.scheduleAtFixedRate(500.millis, 500.millis, self, contactRef ! Ping)
      )      
    }

    case RemoveContact(targetId) => {
      contacts -= targetId
    }

    case GetContacts => {
      sender() ! contacts
    }

    // Appelé régulièrement par la liste elle même pour mettre à jour la liste de contact
    case Ping => {
      sender() ! Pong(id)
    }

    // Réponse reçu après un Ping -> Le sender est bien en vie, on enregistre la dernière date de vérification
case Pong(id) =>
  val now = Insta@@nt.now()
  contacts.get(id).foreach { c =>
    contacts += id -> c.copy(lastSeen = now)
  }
      
  }

}

```


#### Short summary: 

empty definition using pc, found symbol in pc: 