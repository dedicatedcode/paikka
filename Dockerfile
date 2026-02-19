FROM eclipse-temurin:25.0.2_10-jre

LABEL maintainer="dedicatedcode"
LABEL org.opencontainers.image.source="https://github.com/dedicatedcode/paikka"
LABEL org.opencontainers.image.description="Paikka - a specialized reverse geocoding service designed to provide high-performance location resolution"
LABEL org.opencontainers.image.licenses="MIT"

# Create a non-root user and group
RUN userdel ubuntu
RUN adduser paikka --uid 1000

# add osmium-tool and other needed applications
RUN apt update && DEBIAN_FRONTEND=noninteractive apt-get -yq install osmium-tool wget
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

COPY scripts/* $APP_HOME/

RUN ln -s $APP_HOME/filter_osm.sh /usr/bin/prepare
RUN ln -s $APP_HOME/import.sh /usr/bin/import

RUN chmod +x /usr/bin/prepare
RUN chmod +x /usr/bin/import

# Create a script to start the application with configurable UID/GID
RUN cat <<'EOF' > /entrypoint.sh
#!/bin/sh
if [ -n "$APP_UID" ] && [ -n "$APP_GID" ]; then
  echo "Changing paikka user/group to UID:$APP_UID / GID:$APP_GID"
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

# Run as root initially to allow UID/GID changes
USER root

ENTRYPOINT ["/entrypoint.sh"]
