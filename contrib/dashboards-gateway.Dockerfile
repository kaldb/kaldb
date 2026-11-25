# Pin multi-platform image digests so upstream rebuilds cannot silently change the toolchain or OS.
FROM maven:3-amazoncorretto-25@sha256:219984ceca321a578409b7eacc235837a299f96e3433246830df5d506c07dc77 AS build
WORKDIR /work
COPY . /work/
RUN mvn -f /work/contrib/dashboards-gateway/pom.xml -DskipTests package

FROM amazoncorretto:25-al2023@sha256:d4b9047349107b5c5a3c67f89853c48a83d4c769d4196a03f675a4c365737f7b
COPY --from=build /work/contrib/dashboards-gateway/target/tools-dashboards-gateway.jar /dashboards-gateway.jar
# The pinned AL2023 image provides curl-minimal for the HTTP healthcheck.
RUN curl --version
HEALTHCHECK --interval=5s --timeout=5s --start-period=5s --retries=20 \
  CMD curl -fsS http://localhost:9200/_astra/gateway/health
ENTRYPOINT ["java", "-jar", "/dashboards-gateway.jar"]
