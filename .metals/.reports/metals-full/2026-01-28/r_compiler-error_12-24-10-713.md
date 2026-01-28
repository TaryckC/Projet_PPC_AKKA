error id: 9F5B55D8F9FEEEAB7F8153B74B5E6533
file://<WORKSPACE>/src/main/scala/ContactList.scala
### java.lang.IndexOutOfBoundsException: -1

occurred in the presentation compiler.



action parameters:
offset: 792
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

  var contacts = Map[Int, Contact](@@)

  def receive: Receive = {
    case AddContact(id, timeStamp) => {
      contacts += (id -> ref)
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