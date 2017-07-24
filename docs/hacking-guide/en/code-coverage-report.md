# Code coverage report

## Getting JaCoCo exec files

Before you can generate code coverage report by JaCoCo tool,
you need to get data about what lines of code were executed
during testing. These information are collected by JaCoCo
agent and stored in JaCoCo exec files. All you need to do
is run the tests with `jacoco` maven profile.

```
mvn test -Ptests,extra-tests,jacoco
```

## Generate JaCoCo reports

```
mvn verify -Pjacoco-generate-report -DskipTests
```

For generating JaCoCo reports only run the maven build
with profile `jacoco-generate-report` as it is shown
in the example above. After the command was executed,
in directory `target/jacoco-report` you can find
reports in HTML and XML formats.

## Merge JaCoCo exec files to one

Since ActiveMQ Artemis is divided into several modules,
exec files are generated for each module separately.
If you need to merge them together to have all data
in one place, you can do it by command below.

```
mvn jacoco:merge -N -Pjacoco
```

