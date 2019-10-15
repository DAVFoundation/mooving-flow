FROM gradle:jdk8 as cache

USER root
WORKDIR /home/gradle/project

COPY settings.gradle /home/gradle/project
COPY build.gradle /home/gradle/project
RUN gradle --console=plain --info --no-daemon build

FROM cache as builder

USER root
WORKDIR /home/gradle/project

COPY settings.gradle /home/gradle/project
COPY build.gradle /home/gradle/project
COPY src /home/gradle/project/src
RUN gradle --console=plain --info --no-daemon shadowJar

FROM alpine
COPY --from=builder /home/gradle/project/build/libs /jars
ARG VERSION
LABEL VERSION=${VERSION}
