###
#FROM adoptopenjdk/maven-openjdk11 as DEPENDENCIES
#WORKDIR /build
#COPY pom.xml .
#COPY src src
#RUN mvn dependency:go-offline

###
#FROM adoptopenjdk/maven-openjdk11 as BUILDER
#WORKDIR /build
#COPY pom.xml .
#COPY src src
#COPY --from=DEPENDENCIES /root/.m2 /root/.m2
#RUN mvn clean package

###
FROM openjdk:11 as RUNTIME
#COPY --from=BUILDER /build/target/kafka-streams.jar /app.jar
COPY target/kafka-streams-simulators.jar /app.jar
ENTRYPOINT ["java","-jar","/app.jar"]