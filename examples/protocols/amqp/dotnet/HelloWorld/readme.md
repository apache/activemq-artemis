# Running the .NET AMQP example


# Pre-requisites:

All of this can be done on Linux, Mac and... Windows

- Install .NET

https://www.microsoft.com/net/core


- Visual Studio Code is free and may be useful:

https://code.visualstudio.com


- Powershell might be also useful:

https://github.com/PowerShell/PowerShell/



# running the example

- Create the broker, by running:

```
../../../../../bin/artemis create ./server1 --user a --password a --role a --allow-anonymous --force
./server1/bin/artemis-service start
```

Or simply use the start-broker.sh script on this directory


- Compile the code

You need call restore to download AMQP Library and build it.
Restore is part of NuGET which is sort of the Maven Repo for Java devs.

```sh
dotnet restore
dotnet build
dotnet run
```

Or simply use the run-example.sh script on this directory

- Debugging

Visual Studio Code will make it fairly easy to do it
