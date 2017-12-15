# Openwire Chat Example

This example will require multiple windows to be executed

Window 1: 

    mvn -Pserver verify

Window 2: 

    mvn -Pchat1 verify

Window 3:

    mvn -Pchat2 verify

Window 4: 

    mvn -Pchat3 verify

You should be able to interact with the chat application:

## Chat application:
The application user Chatter2 connects to the broker at `tcp://localhost:61616`.

The application will publish messages to the `chat` address.

The application also subscribes to that topic to consume any messages published there.

Type some text, and then press Enter to publish it as a TextMesssage from Chatter2.

    Hello guys
    Chatter2: Hello guys