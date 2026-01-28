error id: file://<WORKSPACE>/src/main/scala/Musicien.scala:currentTimeMillis.
file://<WORKSPACE>/src/main/scala/Musicien.scala
empty definition using pc, found symbol in pc: 
empty definition using semanticdb
empty definition using fallback
non-local guesses:

offset: 935
uri: file://<WORKSPACE>/src/main/scala/Musicien.scala
text:
```scala
package upmc.akka.leader

import akka.actor._

case class Start()

class Musicien(val id: Int, val terminaux: List[Terminal]) extends Actor {

  import ContactListActor._
  import scala.concurrent.duration._
  import scala.util.{Success, Failure}
  import context.dispatcher

  // Objets reçus
  case class Hello(senderId: Int, timeStamp: Long, contactRef: ActorRef)
  case class Hey(
      targetId: Int,
      senderId: Int,
      timeStamp: Long,
      contactRef: ActorRef
  )

  // Instanciation des acteurs
  val myContactList: ActorRef =
    context.actorOf(Props(new ContactListActor(id)), "contactList")

  /*
     Chaque musicien possède :
          - Une liste de contact (ContactList) : Sert à garder à jours qui est vivant, qui est mort, qui est le leader
          -
   */

  // Les differents acteurs du systeme
  val displayActor = context.actorOf(Props[DisplayActor], name = "displayActor")
  val birthDate = System.cu@@rrentTimeMillis()
  var aloneSince: Long = 0
  var isMaster = false
  var aliveContacts: List[Int] = List()

  def receive = {

    // Initialisation
    case Start => {
      displayActor ! Message("Musicien " + this.id + " is created")

      // TODO : Que faire à l'initialisation ?
      // Compteur le nombre de message envoyé

      var numberOfMusicianAtLaunch = 0

      // Il faut essayer de contacter les autres musiciens s'ils existent (régulièrement jusqu'à 30 secondes)
      // Et sinon, à la fin des 30 secondes, s'arrêter.

      for (i <- 0 to 3) {
        val host = terminaux(i).ip
        val port = terminaux(i).port
        val path = s"akka.tcp://MozartSystem$i@127.0.0.1:$port/user/Musicien$i"

        context.actorSelection(path).resolveOne(30.seconds).onComplete {
          case Success(ref) =>
            ref ! Hello(id, birthDate, myContactList) // timestamp est un long
            numberOfMusicianAtLaunch += 1
          case Failure(
                ex
              ) => // TODO : Acteur absent, rien à faire
        }
      }

      if (numberOfMusicianAtLaunch == 0) {
        // Le musicien est le premier arrivé
        isMaster = true

        // Il attends qu'on le contact pendant 30 secondes
        aloneSince = System.currentTimeMillis()
      }

      // Toutes les secondes on va vérifier si on est seul depuis au moins 30 secondes.
      context.system.scheduler.scheduleOnce(1.seconds, self, AmIAlone)

    }

    // Réception d'un message suite à la création d'un musicien (le musicien se présente)
    case Hello(senderId, timeStamp, contactRef) => {
      // On transmet les infos à la liste de contacts pour update
      updateAloneSince()
      // On enregistre le nouveau contacte
      myContactList ! AddContact(
        senderId,
        timeStamp,
        contactRef // contactRef étant la référénce vers la liste de contacte
      )

      // On répond avec un Hey
      sender() ! Hey(senderId, id, birthDate, contactRef)
    }

    // Après Avoir reçu un Hello, on répond avec Hey et on se présente aussi.
    // TODO : on peut aussi transmettre un timestamp qui correspond à notre date de naissance. (Pour établir la priorité des chefs)
    case Hey(myId, senderId, timeStamp, contactRef) => {
      updateAloneSince()
      // On enregistre le nouveau contacte
      myContactList ! AddContact(senderId, timeStamp, contactRef)
    }

    // Appelle automatique toutes les secondes pour vérifier si on est seul depuis trop longtemps
    case AmIAlone => {
      if (System.currentTimeMillis() - aloneSince >= 30 * 1000) {
        // Si seul depuis au moins 30 secondes -> meurt.
        context.stop(self) // RIP
      }
    }

    case getCurrentContacts(contacts) => {
      aliveContacts = contacts
    }
  }

  // Met à jours la dernière valeur de aloneSince
  def updateAloneSince() {
    aloneSince = System.currentTimeMillis()
  }

}

```


#### Short summary: 

empty definition using pc, found symbol in pc: 