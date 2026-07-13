FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY . .

RUN dotnet restore HappyEcho.slnx

RUN dotnet test HappyEcho.slnx \
    --configuration Release \
    --no-restore

RUN dotnet publish HappyEcho/HappyEcho.csproj \
    --configuration Release \
    --no-restore \
    --output /app/publish \
    /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/runtime:10.0 AS runtime
WORKDIR /app

COPY --from=build /app/publish .

USER ${APP_UID}

EXPOSE 7

ENTRYPOINT ["dotnet", "HappyEcho.dll"]
