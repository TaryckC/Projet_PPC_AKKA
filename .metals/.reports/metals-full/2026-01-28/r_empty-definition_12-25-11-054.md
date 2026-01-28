error id: file://<WORKSPACE>/src/main/scala/ContactList.scala:
file://<WORKSPACE>/src/main/scala/ContactList.scala
empty definition using pc, found symbol in pc: 
empty definition using semanticdb
empty definition using fallback
non-local guesses:

offset: 924
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
  ref: ActorRef
)

object ContactListActor {
  case class AddContact(id: Int, timeStamp: String)
  case class RemoveContact(id: Int)
  case object GetContacts
}

class ContactListActor extends Actor {

  /*
    
    La liste de contacte va se charger de communiquer avec les listes de contacts des autres musiciens.
    En éspérant à chaque fois une réponse.
    Si une réponse n'arrive pas au bout d'un certain temps (ou si on reçoit un message de fin par une liste externes)
    On sait que le musicien associé est mort (RIP).

   */

  import ContactListActor._
  import Contact

  var contacts = Map[Int, Contact]()

  def receive: Receive = {
    case AddContact(id, timeStamp) => {
      val now = Instant.now()
      contacts += id -> Contact(@@id,now,now,sender())
    }

    case RemoveContact(id) => {
      contacts -= id
    }

    case GetContacts => {
      sender() ! contacts
    }
  }
}

```


#### Short summary: 

empty definition using pc, found symbol in pc: 