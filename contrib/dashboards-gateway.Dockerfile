# Pin multi-platform image digests so upstream rebuilds cannot silently change the toolchain or OS.
FROM maven:3.10.0-amazoncorretto-21@sha256:f7ccf5b5013cab77ba7f9137f5ce2fb573eb29791069d6cdd9bc9c1486065946 AS build
WORKDIR /work
COPY . /work/
RUN mvn -f /work/contrib/dashboards-gateway/pom.xml -DskipTests package

FROM amazoncorretto:21-al2023@sha256:90100f194336ef2326f5efe1ee747395fd87d8210ad6d60463196aa0c4b0ad4e
COPY --from=build /work/contrib/dashboards-gateway/target/tools-dashboards-gateway.jar /dashboards-gateway.jar
# The pinned AL2023 image provides curl-minimal for the HTTP healthcheck.
RUN curl --version
HEALTHCHECK --interval=5s --timeout=5s --start-period=5s --retries=20 \
  CMD curl -fsS http://localhost:9200/_astra/gateway/health
ENTRYPOINT ["java", "-jar", "/dashboards-gateway.jar"]
