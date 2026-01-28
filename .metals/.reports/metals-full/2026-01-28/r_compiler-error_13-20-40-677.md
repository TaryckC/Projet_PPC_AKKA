error id: 9F5B55D8F9FEEEAB7F8153B74B5E6533
file://<WORKSPACE>/src/main/scala/ContactList.scala
### java.lang.IndexOutOfBoundsException: -1

occurred in the presentation compiler.



action parameters:
offset: 1559
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
    case Pong(id) => {
      contacts(@@)
    }
      
  }

}

```


presentation compiler configuration:
Scala version: 3.3.7-bin-nonbootstrapped
Classpath:
<HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/scala-lang/scala3-library_3/3.3.7/scala3-library_3-3.3.7.jar [exists ], <HOME>/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/scala-lang/scala-library/2.13.16/scala-library-2.13.16.jar [exists ]
Options:





#### Error stacktrace:

```
scala.collection.LinearSeqOps.apply(LinearSeq.scala:129)
	scala.collection.LinearSeqOps.apply$(LinearSeq.scala:128)
	scala.collection.immutable.List.apply(List.scala:79)
	dotty.tools.dotc.util.Signatures$.applyCallInfo(Signatures.scala:244)
	dotty.tools.dotc.util.Signatures$.computeSignatureHelp(Signatures.scala:101)
	dotty.tools.dotc.util.Signatures$.signatureHelp(Signatures.scala:88)
	dotty.tools.pc.SignatureHelpProvider$.signatureHelp(SignatureHelpProvider.scala:46)
	dotty.tools.pc.ScalaPresentationCompiler.signatureHelp$$anonfun$1(ScalaPresentationCompiler.scala:498)
	scala.meta.internal.pc.CompilerAccess.withSharedCompiler(CompilerAccess.scala:149)
	scala.meta.internal.pc.CompilerAccess.withNonInterruptableCompiler$$anonfun$1(CompilerAccess.scala:133)
	scala.meta.internal.pc.CompilerAccess.onCompilerJobQueue$$anonfun$1(CompilerAccess.scala:210)
	scala.meta.internal.pc.CompilerJobQueue$Job.run(CompilerJobQueue.scala:153)
	java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1144)
	java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:642)
	java.base/java.lang.Thread.run(Thread.java:1575)
```
#### Short summary: 

java.lang.IndexOutOfBoundsException: -1