# Custom Kafka image: replaces the bundled Adoptium JDK (crashes with SIGILL
# on Apple Silicon Docker Desktop) with Eclipse Temurin 21 JRE.
# NOTE: apache/kafka:3.7 is Alpine-based → must use the Alpine JRE variant.
#
# Build:  docker-compose build kafka
# Usage:  docker-compose up -d kafka
FROM eclipse-temurin:21-jre-alpine AS temurin

FROM apache/kafka:3.7.0
USER root

# Replace broken Adoptium JDK at /opt/java/openjdk with Temurin 21 (Alpine)
RUN rm -rf /opt/java/openjdk
COPY --from=temurin /opt/java/openjdk /opt/java/openjdk

# JAVA_HOME is already /opt/java/openjdk in the base image — no override needed
USER appuser


