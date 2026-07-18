# Stage 1: Build the native binary using the full SDK + native tools
FROM mcr.microsoft.com/dotnet/sdk:10.0-alpine AS build
WORKDIR /src

# Install the native build tools required by AOT
RUN apk add --no-cache clang build-base zlib-dev

COPY . .
RUN dotnet publish -c Release -r linux-musl-x64 -p:PublishAot=true -o /app/publish

# Stage 2: Run using a bare-minimum image (No .NET runtime installed!)
FROM mcr.microsoft.com/dotnet/runtime-deps:10.0-alpine AS final
WORKDIR /app
COPY --from=build /app/publish .
EXPOSE 7

ENTRYPOINT ["./HappyEcho"]
