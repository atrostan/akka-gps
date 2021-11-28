


Run `assembly` in `sbt-shell`.

[//]: # (# DONT FOLLOW INSTRUCTIONS BELOW)

[//]: # (# Build JARS of Compressor and Partitioning Driver in Intellij)

[//]: # ()
[//]: # (1. Go to `File` > `Project Structure`)

[//]: # (2. Under `Project Settings` sidebar, select `Artifacts`)

[//]: # (3. Click `+` to add a new artifact. We'll create an artifact for the Compressor and an artifact for the Partitioner.)

[//]: # (4. Under `Add`, select `Jar` > `From modules with dependencies`)

[//]: # (5. In `Create JAR from Modules`, )

[//]: # (    1. In `Module:`, select `akka-gps`)

[//]: # (    2. Click on open folder in `Main Class`.)

[//]: # (        - In `Select Main Class`, IntelliJ should detect:)

[//]: # (            - `Driver` `&#40;com.preprocessing.edgelist&#41;`: This is the Compressor.)

[//]: # (            - `Driver` `&#40;com.preprocessing.partitioning&#41;`: This is the partitioner.)

[//]: # (        - Select each, and give them descriptive names, e.g.:)

[//]: # (            - `akka-gps-compressor:jar`, and)

[//]: # (            - `akka-gps-partitioner:jar`)

[//]: # (6. Click `Apply`, `Ok`.)

[//]: # (7. To build, Click `Build` > `Build artifacts...`)

[//]: # (8. Once built, run `scripts/preprocess.sh`:)

[//]: # (    - Set )

[//]: # (        - `partitionDriverJarPath`)

[//]: # (        - `compressorDriverJarPath`)

[//]: # (    - to the locations you've exported them to.)

[//]: # (9. `scripts/preprocess.sh` compresses and partitions a graph using:  )

[//]: # (    1. 1D partitioning by source + by destination)

[//]: # (    2. 2D partitioning by source + by destination)

[//]: # (    3. Hybrid partitioning by source + by destination given a threshold.)

[//]: # ()
[//]: # (## Notes)

[//]: # (Remove `project/META-INF/MANIFEST.MF` after building the artefacts, otherwise IntelliJ will complain.)
