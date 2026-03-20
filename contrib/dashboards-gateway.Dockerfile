FROM maven:3-amazoncorretto-21 AS build
WORKDIR /work
COPY . /work/
RUN mvn -f /work/contrib/dashboards-gateway/pom.xml -DskipTests package

FROM amazoncorretto:21
COPY --from=build /work/contrib/dashboards-gateway/target/tools-dashboards-gateway.jar /dashboards-gateway.jar
# Current amazoncorretto:21 images appear to include curl, but install it explicitly so the
# healthcheck remains stable across base image refreshes.
RUN yum install -y curl && yum clean all
HEALTHCHECK --interval=5s --timeout=5s --start-period=5s --retries=20 \
  CMD curl -fsS http://localhost:9200/_astra/gateway/health
ENTRYPOINT ["java", "-jar", "/dashboards-gateway.jar"]
