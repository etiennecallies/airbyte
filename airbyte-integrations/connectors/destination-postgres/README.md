### Build and push docker image

Have java21 installed. Example with mise:
```shell
mise use -g java@21
```

Then in airbyte main folder, run:
```shell
./gradlew :airbyte-integrations:connectors:destination-postgres:build
docker tag airbyte/destination-postgres:dev etiennecalliesouihelp/airbyte-destination-postgres:{new-tag}
docker login # if not already logged
docker push etiennecalliesouihelp/airbyte-destination-postgres:{new-tag}
```