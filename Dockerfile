FROM openjdk:17-alpine3.14 as build
LABEL authors="theo"

WORKDIR application/rinha

COPY src src

COPY ./gradlew ./gradlew

COPY gradle gradle

COPY build.gradle build.gradle

COPY settings.gradle settings.gradle

RUN ./gradlew install && ./gradlew build

FROM build as prod

LABEL authors="theo"

COPY --from=build build/libs/*.jar build

ENTRYPOINT ["java --jar build"]

