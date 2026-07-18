# Stage 1: Build and test the native binary
FROM mcr.microsoft.com/dotnet/sdk:10.0-alpine AS build
WORKDIR /src

RUN apk add --no-cache \
    clang \
    build-base \
    zlib-dev

COPY . .

RUN dotnet restore HappyEcho.slnx

RUN dotnet test HappyEcho.slnx \
    --configuration Release \
    --no-restore

RUN dotnet publish HappyEcho/HappyEcho.csproj \
    --configuration Release \
    --runtime linux-musl-x64 \
    --self-contained true \
    /p:PublishAot=true \
    --no-restore \
    --output /app/publish

# Stage 2: Native executable only
FROM mcr.microsoft.com/dotnet/runtime-deps:10.0-alpine AS final
WORKDIR /app

COPY --from=build /app/publish .

EXPOSE 7

ENTRYPOINT ["./HappyEcho"]