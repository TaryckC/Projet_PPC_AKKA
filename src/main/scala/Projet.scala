package upmc.akka.leader

import com.typesafe.config.ConfigFactory
import akka.actor._

import java.net.ServerSocket
import scala.util.Try

case class Terminal(id: Int, ip: String, port: Int)

object Projet {

  def isPortAvailable(port: Int): Boolean = {
    // On essaie d'ouvrir le port. Si ça réussit, on le ferme et on renvoie true.
    val socketTry = Try(new ServerSocket(port))
    socketTry.foreach(_.close())
    socketTry.isSuccess
  }

  def main(args: Array[String]): Unit = {
    // Gestion des erreurs
    if (args.size != 1) {
      println("Erreur de syntaxe : run <num>")
      sys.exit(1)
    }

    val id: Int = args(0).toInt

    if (id < 0 || id > 3) {
      println("Erreur : <num> doit etre compris entre 0 et 3")
      sys.exit(1)
    }

    var musicienlist = List[Terminal]()

    // recuperation des adresses de tous les musiciens
    // hardcoded path name
    for (i <- 3 to 0 by -1) {
      val address = ConfigFactory
        .load()
        .getConfig("system" + i)
        .getValue("akka.remote.netty.tcp.hostname")
        .render()
      val port = ConfigFactory
        .load()
        .getConfig("system" + i)
        .getValue("akka.remote.netty.tcp.port")
        .render()
      musicienlist = Terminal(i, address, port.toInt) :: musicienlist
    }

    val targetPort = ConfigFactory.load()
      .getConfig("system" + id)
      .getInt("akka.remote.netty.tcp.port")

    // Vérifier si le port est libre - Si on essaye d'ajouter deux contacts avec le même id
    if (!isPortAvailable(targetPort)) {
      println(s"--- ERREUR ---")
      println(s"Le musicien $id ne peut pas être lancé car le port $targetPort est déjà occupé.")
      println(s"Une autre instance du musicien $id est probablement déjà en cours d'exécution.")
      sys.exit(1)
    }

    println(musicienlist)

    // Initialisation du node <id>
    val system = ActorSystem(
      "MozartSystem" + id,
      ConfigFactory.load().getConfig("system" + id)
    )
    val musicien =
      system.actorOf(Props(new Musicien(id, musicienlist)), "Musicien" + id)

    musicien ! Start

    // Ajout pour garder le programme vivant
    println("Appuyez sur Entrée pour arrêter le programme")
    scala.io.StdIn.readLine()

    println("Arrêt du système...")
    system.terminate()

  }

}
