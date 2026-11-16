#See https://aka.ms/customizecontainer to learn how to customize your debug container and how Visual Studio uses this Dockerfile to build your images for faster debugging.

# tini built from source, as in ArmoniK.Core: the release binary buffers its own output,
# so its -vv logs are delayed, or lost when the container is SIGKILLed
FROM alpine:3.24.3 AS tini-build
RUN apk add gcc cmake make musl-dev
ADD https://github.com/krallin/tini.git .

# By default, tini uses buffered IO. Disable buffering to have logs in real-time
RUN sed -i '/int main/a setvbuf(stdout, NULL, _IOLBF, BUFSIZ);setvbuf(stderr, NULL, _IOLBF, BUFSIZ);' src/tini.c
RUN cmake . && make

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS base
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
RUN groupadd --gid 5000 armonikuser && useradd --home-dir /home/armonikuser --create-home --uid 5000 --gid 5000 --shell /bin/sh --skel /dev/null armonikuser
RUN mkdir /cache && chown armonikuser: /cache
USER armonikuser
EXPOSE 1080
ENTRYPOINT ["/tini", "-s", "-vv", "--", "dotnet", "ArmoniK.Extensions.CSharp.DynamicWorker.dll"]