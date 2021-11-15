import sbt._
// imports standard command parsing functionality
import complete.DefaultParsers._
import sbt.Keys._

// imports standard command parsing functionality

object CommandExample {

  def hello = Command.command("hello") { state =>
    println("Hi!")
    state
  }

  // Demonstration of a custom parser.
  // The command changes the foreground or background terminal color
  //  according to the input.
  lazy val change = Space ~> (reset | setColor)
  lazy val reset = token("reset" ^^^ "\033[0m")
  lazy val color = token(Space ~> ("blue" ^^^ "4" | "green" ^^^ "2"))
  lazy val select = token("fg" ^^^ "3" | "bg" ^^^ "4")
  lazy val setColor = (select ~ color) map { case (g, c) => "\033[" + g + c + "m" }

  def changeColor = Command("color")(_ => change) { (state, ansicode) =>
    print(ansicode)
    state
  }

  def partitionBySource1D = Command.command("partitionBySource1D") { state =>
    println("partitionbysource1d")
    def buildRunMainStr(
        nNodes: Int,
        nEdges: Int,
        inputFilename: String,
        outputDirectoryName: String,
        sep: String,
        partitioner: Int,
        threshold: Int,
        numPartitions: Int,
        partitionBySource: Int
    ): String = {
      var s = " com.preprocessing.partitioning.Driver"
      s += s"--nNodes ${nNodes} "
      s += s"--nEdges ${nEdges} "
      s += s"--inputFilename ${inputFilename} "
      s += s"--outputDirectoryName ${outputDirectoryName} "
      s += s"--sep ${sep} "
      s += s"--partitioner ${partitioner} "
      s += s"--threshold ${threshold} "
      s += s"--numPartitions ${numPartitions} "
      s += s"--partitionBySource ${partitionBySource} "
      s
    }
    val taskInStr =
      " com.preprocessing.partitioning.Driver --nNodes 8 --nEdges 32 --inputFilename \"src/main/resources/graphs/8rmat\" --outputDirectoryName \"src/main/resources/graphs/8rmat/partitions\" --sep \" \" --partitioner 3 --threshold 100 --numPartitions 4 --partitionBySource 0"

    val runMainStr = buildRunMainStr(
      8,
      32,
      "src/main/resources/graphs/8rmat",
      "src/main/resources/graphs/8rmat/partitions",
      " ",
      3,
      100,
      4,
      0
    )
    println(runMainStr)
    val myRun = taskKey[Unit]("...")

    myRun := Def.taskDyn {
      val appName = name.value
      Def.task {
        (runMain in Compile)
          .toTask(s" com.softwaremill.MyMain $appName")
          .value
      }
    }.value

    TaskKey[Unit]("myTask") := (runMain in Compile).toTask(" com.example.Main arg1 arg2").value


    //    Project.runTask(myRun)
    state
  }
}
