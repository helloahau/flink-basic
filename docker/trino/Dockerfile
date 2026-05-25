# Custom Trino image: replaces the bundled JDK (which crashes on Apple Silicon
# Docker Desktop with SIGILL) with Eclipse Temurin 21 (proper ARM64 support).
#
# Build:  docker-compose build trino
# Usage:  docker-compose up -d trino   (uses built image automatically)
FROM eclipse-temurin:21-jre-jammy AS temurin

FROM trinodb/trino:443
USER root

# Remove the broken Adoptium JDK that ships with this image
RUN rm -rf /usr/lib/jvm/jdk-21.0

# Copy Eclipse Temurin 21 JRE (ARM64-safe)
COPY --from=temurin /opt/java/openjdk /usr/lib/jvm/temurin-21

# Point Trino launcher at the new JDK
ENV JAVA_HOME=/usr/lib/jvm/temurin-21
ENV PATH=/usr/lib/jvm/temurin-21/bin:$PATH

USER trino

