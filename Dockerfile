# Extend from the existing xdbc-client image
FROM xdbc-client:latest

# Set environment variables for non-interactive installation
ENV DEBIAN_FRONTEND=noninteractive

# Install necessary tools: curl, apt-transport-https, gnupg
RUN apt-get update && apt-get install -y \
    apt-transport-https \
    curl \
    gnupg \
    && apt-get clean

# Add the SBT repositories and import the GPG key
RUN echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list && \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | tee /etc/apt/sources.list.d/sbt_old.list && \
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | gpg --no-default-keyring --keyring gnupg-ring:/etc/apt/trusted.gpg.d/scalasbt-release.gpg --import && \
    chmod 644 /etc/apt/trusted.gpg.d/scalasbt-release.gpg

# Install OpenJDK (Java Development Kit)
RUN apt-get update && apt-get install -y openjdk-11-jdk && apt-get clean

# Install SBT
RUN apt-get update && apt-get install -y sbt && apt-get clean

# Download and install Spark
RUN mkdir /spark && \
    curl -o /tmp/spark-3.3.1-bin-hadoop3.tgz https://archive.apache.org/dist/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3.tgz && \
    tar -xvzf /tmp/spark-3.3.1-bin-hadoop3.tgz -C /spark --strip-components=1 && \
    rm /tmp/spark-3.3.1-bin-hadoop3.tgz

RUN mkdir /tmp/spark-events

# Download postgres driver for spark
RUN curl -o /spark/jars/postgresql-42.7.3.jar https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar
RUN curl -o /spark/jars/play-json_2.12-2.9.2.jar https://repo.akka.io/maven/com/typesafe/play/play-json_2.12/2.9.2/play-json_2.12-2.9.2.jar
RUN curl -o /spark/jars/play-iteratees_2.12-2.9.2.jar https://repo.akka.io/maven/com/typesafe/play/play-iteratees_2.12/2.9.2/play-iteratees_2.12-2.9.2.jar
RUN curl -o /spark/jars/play-functional_2.12-2.9.2.jar https://repo.akka.io/maven/com/typesafe/play/play-functional_2.12/2.9.2/play-functional_2.12-2.9.2.jar

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Set environment variables for Spark
ENV SPARK_HOME=/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Create a directory for the project inside the container
WORKDIR /app
COPY src /app/src
COPY project/build.properties /app/project/
COPY project/plugins.sbt /app/project/
COPY run_xdbc_spark.sh /app
COPY build.sbt /app

#RUN sbt javah
RUN sbt package

# Mount your working directory later when running the container
# This will allow you to use your local code with this image
# and compile/run everything inside the container.

# Default command for container (change it as per your needs)
ENTRYPOINT ["/bin/bash"]
