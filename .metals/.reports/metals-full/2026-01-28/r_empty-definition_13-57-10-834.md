error id: file://<WORKSPACE>/src/main/scala/ContactList.scala:upmc/akka/leader/ContactListActor#PingIntervalMs.
file://<WORKSPACE>/src/main/scala/ContactList.scala
empty definition using pc, found symbol in pc: 
empty definition using semanticdb
empty definition using fallback
non-local guesses:

offset: 763
uri: file://<WORKSPACE>/src/main/scala/ContactList.scala
text:
```scala
package upmc.akka.leader

import akka.actor._

import akka.actor.ActorRef
import java.time.Instant

case class Contact(
  id: Int,
  birthDate: Long,
  lastSeen: Long,
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
  import Contact.–

  val PingInte@@rvalMs = 500

  var contacts = Map[Int, Contact]()

  def receive: Receive = {
    case AddContact(targetId, timeStamp, contactRef) => {
      // TODO : Cas à gérer - Si on essaye d'ajouter deux contacts avec le même id
      val now = Instant.now()
      contacts += targetId -> Contact(
        targetId, 
        now, 
        now, 
        contactRef, 
        context.system.scheduler.scheduleAtFixedRate(PingIntervalMs.millis, PingIntervalMs.millis, self, contactRef ! Ping)
      )      
    }

    case RemoveContact(targetId) => {
      contacts -= targetId
    }

    case GetContacts => {
      // Retourne les ids des musiciens présents dans la liste
      sender() ! contacts
    }

    // Appelé régulièrement par la liste elle même pour mettre à jour la liste de contact
    case Ping => {
      sender() ! Pong(id)
      updateContactList()
    }

    // Réponse reçu après un Ping -> Le sender est bien en vie, on enregistre la dernière date de vérification
    case Pong(senderId) => {
      contacts(senderId).lastSeen = System.currentTimeMillis()
    }
  }

  // Parcours la liste de contacts et retire tout ceux qui sont innactifs de puis 2 * la durée d'un ping
  def updateContactList() {
    print("Contact list update")
    var contactToRemove: List[Int] = List()
    contacts.foreach {
      case (id, c) =>
        if (System.currentTimeMillis() - c.lastSeen >= 2 * PingIntervalMs) {
          contactToRemove += id
        }
    }
    for (id <- contactToRemove) {
      contacts -= id
    }
  }

}

```


#### Short summary: 

empty definition using pc, found symbol in pc: 