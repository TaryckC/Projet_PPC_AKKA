package upmc.akka.leader

import akka.actor._

import akka.actor.ActorRef
import java.time.Instant
import scala.concurrent.duration._

case class Contact(
    id: Int,
    birthDate: Long,
    lastSeen: Long,
    ref: ActorRef,
    isMaster: Boolean
)

object ContactListActor {
  case class AddContact(
      id: Int,
      timeStamp: Long,
      contactRef: ActorRef,
      isMaster: Boolean
  )
  case object GetContacts
  case class Tick(targetId: Int)
  case object Ping
  case class Pong(senderId: Int)
  case object UpdateContactList
  case object BecomeMaster
}

class ContactListActor(val ownerId: Int, onwerRef: ActorRef, birthDate: Long)
    extends Actor {

  /*
    
    La liste de contacte va se charger de communiquer avec les listes de contacts des autres musiciens.
    Elle maintient à jours une liste permettant de savoir qui est encore en vie

    Elle communique à son propriétaire la liste des id valide pour la communication

   */

  import ContactListActor._
  import Musicien._
  import context.dispatcher // ExecutionContext implicite pour le scheduler

  val PingInterval: FiniteDuration = 500.millis
  val contactListUpdateInterval: FiniteDuration = 500.millis

  var contacts = Map[Int, Contact]()
  var isMaster = false

  // Action à effectuer au lancement de l'objet
  override def preStart(): Unit = {
    context.system.scheduler.scheduleOnce(
      contactListUpdateInterval,
      self,
      UpdateContactList
    )
  }

  def receive: Receive = {

    case AddContact(targetId, timeStamp, contactRef, isMaster) => {
      print("\nLe musicien " + targetId + " est déjà là !")
      // TODO : Cas à gérer - Si on essaye d'ajouter deux contacts avec le même id
      val now = System.currentTimeMillis()
      contacts += targetId -> Contact(
        targetId,
        timeStamp,
        now,
        contactRef,
        isMaster
      )
      context.system.scheduler.scheduleOnce(
        PingInterval,
        contactRef,
        Ping
      )(context.dispatcher, self)
    }

    case GetContacts => {
      // Retourne les ids des musiciens présents dans la liste
      sender() ! contacts
    }

    // Appelé régulièrement par la liste elle même pour mettre à jour la liste de contact
    case Ping => {
      sender() ! Pong(ownerId)
    }

    // Réponse reçu après un Ping -> Le sender est bien en vie, on enregistre la dernière date de vérification
    case Pong(senderId) => {
      // print("\nMessage reçu de " + senderId)
      val c = contacts(senderId)
      contacts += senderId -> c.copy(
        lastSeen = System.currentTimeMillis()
      )
      context.system.scheduler.scheduleOnce(
        PingInterval,
        c.ref,
        Ping
      )(context.dispatcher, self)
    }

    case UpdateContactList => {
      updateContactList()
    }

    case ContactListActor.BecomeMaster => {
      isMaster = true
      onwerRef ! Musicien.BecomeMaster
    }

  }

  // Parcours la liste de contacts et retire tout ceux qui sont innactifs de puis 2 * la durée d'un ping
  def updateContactList() {
    var contactToRemove: List[Int] = List()
    var isMasterSet = isMaster
    var oldestMusicianBirthDate: Long = birthDate
    var oldestMusicien: Option[Contact] = None
    contacts.foreach { case (id, c) =>
      if (
        System.currentTimeMillis() - c.lastSeen >= 2 * PingInterval.toMillis
      ) {
        contactToRemove = id :: contactToRemove
      } else {
        isMasterSet = isMasterSet || c.isMaster
        if (oldestMusicianBirthDate > c.birthDate) {
          oldestMusicianBirthDate = c.birthDate
          oldestMusicien = Some(c)
        }
      }
    }

    for (id <- contactToRemove) {
      print("\nLe musicien " + id + " est parti ...")
      contacts -= id
    }

    // print("\n Master status " + (isMaster || isMasterSet))
    // On choisis un nouveau master s'il n'y en a pas
    if (!isMasterSet) {
      oldestMusicien match {
        case None =>
          if (!isMaster)
            self ! ContactListActor.BecomeMaster
          print("I became master")
        case Some(oldest) =>
          // On demande au plus ancien (par birthDate) de devenir master
          contacts = contacts.updated(
            oldest.id,
            contacts(oldest.id).copy(isMaster = true)
          )
          oldest.ref ! ContactListActor.BecomeMaster
          print("\nMaster is " + oldest.id)
      }
    }

    // Appelle récursif
    context.system.scheduler.scheduleOnce(
      contactListUpdateInterval,
      self,
      UpdateContactList
    )

    if (contacts.nonEmpty) {
      onwerRef ! AloneSince // On est pas tout seul
    }

  }

}
