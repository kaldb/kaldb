FROM maven:3-amazoncorretto-25 AS build
WORKDIR /work
COPY . /work/
RUN mvn -f /work/contrib/dashboards-gateway/pom.xml -DskipTests package

FROM amazoncorretto:25
COPY --from=build /work/contrib/dashboards-gateway/target/tools-dashboards-gateway.jar /dashboards-gateway.jar
# Install curl explicitly so the healthcheck remains stable across base image refreshes.
RUN yum install -y curl-minimal && yum clean all
HEALTHCHECK --interval=5s --timeout=5s --start-period=5s --retries=20 \
  CMD curl -fsS http://localhost:9200/_astra/gateway/health
ENTRYPOINT ["java", "-jar", "/dashboards-gateway.jar"]
