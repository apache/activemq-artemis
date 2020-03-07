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

- Create and start the broker, by running:

```bash
./start-server.sh
```

This broker is created by simply using the CLI. you may do it manually if you like:

```bash
artemis create ./server1 --user a --password a --role a --allow-anonymous --force
cd server1/bin
./artemis run
```

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


# About this example

This is sending messages, limited to 25K messages a second.
The consumer will have a pool of consumers, which will synchronously acknowledge messages.
.NET threading model is expensive, this example shows how to make most of your resources by a pool of consumers.
