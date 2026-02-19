FROM eclipse-temurin:25-jre-alpine-3.22

LABEL maintainer="dedicatedcode"
LABEL org.opencontainers.image.source="https://github.com/dedicatedcode/paikka"
LABEL org.opencontainers.image.description="Paikka - a specialized reverse geocoding service designed to provide high-performance location resolution"
LABEL org.opencontainers.image.licenses="MIT"

# Create a non-root user and group
RUN addgroup -S paikka -g 1000 && adduser -S paikka -u 1000 -G paikka

# Set environment variables
ENV SPRING_PROFILES_ACTIVE=docker
ENV APP_HOME=/app
ENV DATA_DIR=/data

# Create application directory
RUN mkdir -p $APP_HOME && \
    chown -R paikka:paikka $APP_HOME

WORKDIR $APP_HOME

# Copy the application jar
COPY --chown=paikka:paikka target/*.jar $APP_HOME/app.jar

COPY scripts/filter_osm.sh $APP_HOME/prepare-ebf
COPY scripts/import.sh $APP_HOME/import
RUN chmod +x $APP_HOME/prepare-ebf
RUN chmod +x $APP_HOME/import
# Create a script to start the application with configurable UID/GID
RUN cat <<'EOF' > /entrypoint.sh
#!/bin/sh
if [ -n "$APP_UID" ] && [ -n "$APP_GID" ]; then
  echo "Changing paikka user/group to UID:$APP_UID / GID:$APP_GID"
  apk add --no-cache shadow
  groupmod -g $APP_GID paikka
  usermod -u $APP_UID paikka
  chown -R paikka:paikka $APP_HOME
fi

mkdir -p $DATA_DIR
chown -R paikka:paikka $DATA_DIR

# Execute
exec su-exec paikka java $JAVA_OPTS -jar $APP_HOME/app.jar -Dspring.profiles.active=docker "$@"
EOF

RUN chmod +x /entrypoint.sh
# Expose the application port
EXPOSE 8080

# Add healthcheck
HEALTHCHECK --interval=5s --timeout=3s --start-period=1s --retries=20 \
  CMD wget --no-verbose --tries=1 --spider http://localhost:8080/actuator/health || exit 1

# Install su-exec for proper user switching and wget for healthcheck
RUN apk add --no-cache su-exec wget attr
RUN setfattr -n user.pax.flags -v "mr" /opt/java/openjdk/bin/java

# Run as root initially to allow UID/GID changes
USER root

ENTRYPOINT ["/entrypoint.sh"]
