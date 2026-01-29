package upmc.akka.leader

import akka.actor._

case class Start()

object Musicien {
  case class Hello(
      senderId: Int,
      timeStamp: Long,
      contactRef: ActorRef,
      isMaster: Boolean
  )
  case class Hey(
      targetId: Int,
      senderId: Int,
      timeStamp: Long,
      contactRef: ActorRef,
      isMaster: Boolean
  )
  case object AmIAlone
  case class GetCurrentContacts(contactList: List[Int])
  case object AloneSince
  case object BecomeMaster
  case class PlayMeasure(measure: DataBaseActor.Measure)
}

class Musicien(val id: Int, val terminaux: List[Terminal]) extends Actor {

  import ContactListActor._
  import scala.concurrent.duration._
  import scala.util.{Success, Failure}
  import context.dispatcher
  import Musicien._
  import ConductorActor._

  // Instanciation des acteurs
  val myContactList: ActorRef =
    context.actorOf(
      Props(new ContactListActor(id, self, System.currentTimeMillis())),
      "contactList"
    )

  /*
     Chaque musicien possède :
          - Une liste de contact (ContactList) : Sert à garder à jours qui est vivant, qui est mort, qui est le leader
          -
   */

  // Les differents acteurs du systeme
  val displayActor = context.actorOf(Props[DisplayActor], name = "displayActor")
  val birthDate = System.currentTimeMillis()
  var aloneSince: Long = 0
  var amIMaster = false
  var conductorRef: Option[ActorRef] = None
  var playerRef: Option[ActorRef] =
    Some(
      context.actorOf(Props[PlayerActor], name = s"player-$id")
    ) // Par défaut on est un 'player'
  var aliveContacts: List[Int] = List()

  // TODO : Ajouter une protection pour quand on lance trop vite et que des message échoue alors qu'ils ne devraient pas.
  // -> Réessayer de contacter toutes les x ms pendant y ms (au lancement uniquement) en cas d'échec

  def receive = {

    // Initialisation
    case Start => {
      displayActor ! Message("Musicien " + this.id + " is created")

      // TODO : Que faire à l'initialisation ?
      // Compteur le nombre de message envoyé

      var numberOfMusicianAtLaunch = 0

      // Il faut essayer de contacter les autres musiciens s'ils existent (régulièrement jusqu'à 30 secondes)
      // Et sinon, à la fin des 30 secondes, s'arrêter.

      for (i <- 0 to 3 if i != id) {
        val host = terminaux(i).ip
        val port = terminaux(i).port
        val path = s"akka.tcp://MozartSystem$i@127.0.0.1:$port/user/Musicien$i"

        context.actorSelection(path).resolveOne(30.seconds).onComplete {
          case Success(ref) =>
            ref ! Hello(
              id,
              birthDate,
              myContactList,
              amIMaster
            ) // timestamp est un long
            numberOfMusicianAtLaunch += 1
          case Failure(
                ex
              ) => // TODO : Acteur absent, rien à faire
        }
      }

      updateAloneSince()

      // Toutes les secondes on va vérifier si on est seul depuis au moins 30 secondes.
      context.system.scheduler.scheduleOnce(1.seconds, self, AmIAlone)

    }

    // Réception d'un message suite à la création d'un musicien (le musicien se présente)
    case Hello(senderId, timeStamp, contactRef, isMaster) => {
      // print("\nHello de la part du musicien " + senderId)
      // On transmet les infos à la liste de contacts pour update
      // On enregistre le nouveau contacte
      myContactList ! AddContact(
        senderId,
        timeStamp,
        contactRef, // contactRef étant la référénce vers la liste de contacte
        isMaster
      )

      // On répond avec un Hey
      sender() ! Hey(senderId, id, birthDate, myContactList, amIMaster)
    }

    // Après Avoir reçu un Hello, on répond avec Hey et on se présente aussi.
    // TODO : on peut aussi transmettre un timestamp qui correspond à notre date de naissance. (Pour établir la priorité des chefs)
    case Hey(myId, senderId, timeStamp, contactRef, isMaster) => {
      // print("\nHey de la part du musicien " + senderId)
      // On enregistre le nouveau contacte
      myContactList ! AddContact(senderId, timeStamp, contactRef, isMaster)
    }

    // Appelle automatique toutes les secondes pour vérifier si on est seul depuis trop longtemps
    case AmIAlone => {
      if (
        aliveContacts.length == 0 && System
          .currentTimeMillis() - aloneSince >= 30 * 1000
      ) {
        // Si seul depuis au moins 30 secondes -> meurt.
        // print("\nLe musicien est mort...")
        context.stop(self) // RIP
      }
      context.system.scheduler.scheduleOnce(1.seconds, self, AmIAlone)
    }

    case GetCurrentContacts(contacts) => {
      aliveContacts = contacts
      if (amIMaster) {
        // Encore une fois ici aussi, le foreach c'est si jamais on a None en conductor
        conductorRef.foreach(_ ! UpdateMusicians(contacts))
      }
    }

    case AloneSince => {
      updateAloneSince()
    }

    case Musicien.BecomeMaster => {
      print("\n Has become master")
      if (!amIMaster) {
        amIMaster = true
        playerRef.foreach(context.stop) // On arrête le player
        playerRef =
          None // On n'a plus besoin de la référence vers le player car on ne peut pas devenir player à nouveau
        // On établit les connexions avec les autres acteurs nécessaires au bon fonctionnement du conductor
        val database =
          context.actorOf(Props[DataBaseActor], name = s"database-$id")
        val provider =
          context.actorOf(
            Props(new ProviderActor(database)),
            name = s"provider-$id"
          )
        val conductor =
          context.actorOf(
            Props(new Conductor(provider, terminaux, id)),
            name = s"conductor-$id"
          )
        conductorRef = Some(conductor)
        conductor ! UpdateMusicians(aliveContacts)
        conductor ! StartGame
      }
    }

    // On délègue le fait de jouer de la musique
    case PlayMeasure(measure) => {
      playerRef.foreach(_ ! measure)
    }
  }

  // Met à jours la dernière valeur de aloneSince
  def updateAloneSince() {
    aloneSince = System.currentTimeMillis()
  }

}
