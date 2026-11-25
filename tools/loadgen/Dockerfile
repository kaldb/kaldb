# ---- build ----
FROM maven:3-amazoncorretto-25 AS builder
WORKDIR /src
COPY pom.xml .
RUN mvn -q -e -DskipTests dependency:resolve
COPY src ./src
RUN mvn -q -DskipTests package

# ---- run ----
FROM amazoncorretto:25 AS run
WORKDIR /app
COPY --from=builder /src/target/tools-loadgen.jar /app/loadgen.jar
ENV JAVA_OPTS="-XX:MaxRAMPercentage=75 -XX:+UseZGC"
ENTRYPOINT ["sh","-c","java $JAVA_OPTS -jar /app/loadgen.jar"]
