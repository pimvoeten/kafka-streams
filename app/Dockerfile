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
FROM eclipse-temurin:11.0.12_7-jdk as RUNTIME
#COPY --from=BUILDER /build/target/kafka-streams.jar /app.jar
EXPOSE 9001 9010

COPY target/kafka-streams.jar /app.jar
WORKDIR /
#ENTRYPOINT ["java", "-jar", "app.jar"]
ENTRYPOINT ["sh", "-c", "java ${JAVA_OPTS} -jar app.jar"]
