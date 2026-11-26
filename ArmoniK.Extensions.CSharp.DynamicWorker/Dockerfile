#See https://aka.ms/customizecontainer to learn how to customize your debug container and how Visual Studio uses this Dockerfile to build your images for faster debugging.

# Runtime base image, overridable to build the image variants:
#   --build-arg BASE_IMAGE=mcr.microsoft.com/dotnet/aspnet:10.0-resolute-chiseled-extra
#   --build-arg BASE_IMAGE=mcr.microsoft.com/dotnet/aspnet:10.0-azurelinux4.0-distroless-extra
# Chiseled and distroless images have no shell: the final stage must not contain any RUN instruction.
ARG BASE_IMAGE=mcr.microsoft.com/dotnet/aspnet:10.0

# tini built from source, as in ArmoniK.Core: the release binary buffers its own output,
# so its -vv logs are delayed, or lost when the container is SIGKILLed
FROM alpine:3.24.3 AS tini-build
RUN apk add gcc cmake make musl-dev
ADD https://github.com/krallin/tini.git .

# By default, tini uses buffered IO. Disable buffering to have logs in real-time.
# The check fails the build if the anchor matched no line, or several, in a future tini.c
RUN sed -i '/int main/a setvbuf(stdout, NULL, _IOLBF, BUFSIZ);setvbuf(stderr, NULL, _IOLBF, BUFSIZ);' src/tini.c && \
    [ "$(grep -c 'setvbuf(stdout' src/tini.c)" -eq 1 ]
RUN cmake . && make

# Empty directory, copied with the right owner into the final image to create /cache and the home directory
RUN mkdir /empty

FROM ${BASE_IMAGE} AS base
WORKDIR /app
EXPOSE 80
EXPOSE 443

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["ArmoniK.Extensions.CSharp.DynamicWorker/ArmoniK.Extensions.CSharp.DynamicWorker.csproj", "ArmoniK.Extensions.CSharp.DynamicWorker/"]
RUN dotnet restore "./ArmoniK.Extensions.CSharp.DynamicWorker/ArmoniK.Extensions.CSharp.DynamicWorker.csproj"
COPY . .
WORKDIR "/src/ArmoniK.Extensions.CSharp.DynamicWorker"
RUN dotnet build "./ArmoniK.Extensions.CSharp.DynamicWorker.csproj" -c $BUILD_CONFIGURATION -o /app/build

FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "./ArmoniK.Extensions.CSharp.DynamicWorker.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
# tini runs as PID 1: it forwards signals to the worker and reaps zombie processes
COPY --from=tini-build /tini-static /tini
COPY --from=publish /app/publish .
# The worker runs as uid/gid 5000 on every variant. No user is declared in /etc/passwd
# (useradd needs a shell, absent from chiseled and distroless images), so HOME is set explicitly.
COPY --from=tini-build --chown=5000:5000 /empty /home/armonikuser
COPY --from=tini-build --chown=5000:5000 /empty /cache
ENV HOME=/home/armonikuser
USER 5000:5000
EXPOSE 1080
ENTRYPOINT ["/tini", "-s", "-vv", "--", "dotnet", "ArmoniK.Extensions.CSharp.DynamicWorker.dll"]
