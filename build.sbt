enablePlugins(JniNative)
name := "spark3io"

version := "1.0"

scalaVersion := "2.12.15"
crossPaths := false
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.1" % Provided
libraryDependencies += "com.typesafe.play" %% "play-json" % "2.9.2" % Provided

target / javah := sourceDirectory.value / "native" / "include"
/*lazy val root = (project in file(".")).aggregate(core, native)


lazy val core = project
  .settings(libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.1" % Provided)
  .settings(javah / target := (native / nativeCompile / sourceDirectory).value / "include")
  .dependsOn(native % Runtime)

lazy val native = project
  .settings(nativeCompile / sourceDirectory := sourceDirectory.value)
  .enablePlugins(JniNative)*/

//target in javah := sourceDirectory.value // / "native" / "include"
//crossPaths := false
//enablePlugins(JniNative)
/*lazy val root = (project in file(".")).aggregate(core, native).
  settings(
    assembly / mainClass := Some("dbreader.ReadPG")
  )

lazy val core = project
  .settings(libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.1" % Provided)
  .settings(javah / target := (native / nativeCompile / sourceDirectory).value / "include")
  .dependsOn(native % Runtime)
*/