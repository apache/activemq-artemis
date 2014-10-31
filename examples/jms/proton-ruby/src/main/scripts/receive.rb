#!/usr/bin/env ruby

require 'qpid_proton'

messenger = Qpid::Proton::Messenger.new()
messenger.incoming_window = 10

begin
  messenger.start
rescue ProtonError => error
  puts "ERROR: #{error.message}"
  puts error.backtrace.join("\n")
  exit
end

  begin
    messenger.subscribe("127.0.0.1:5672/testQueue")
  rescue Qpid::Proton::ProtonError => error
    puts "ERROR: #{error.message}"
    exit
  end

msg = Qpid::Proton::Message.new

  begin
    messenger.receive(10)
  rescue Qpid::Proton::ProtonError => error
    puts "ERROR: #{error.message}"
    exit
  end

  while messenger.incoming.nonzero?
    begin
      messenger.get(msg)
      # for 0.5:
      messenger.accept()

      # for 0.4:
      #messenger.accept(messenger.incoming_tracker, 0)#1 would mean cumulative

      # optional and the same in both versions (messenger will
      # settle itself when tracker passes out the window)
      messenger.settle(messenger.incoming_tracker, 0)


    rescue Qpid::Proton::Error => error
      puts "ERROR: #{error.message}"
      exit
    end

    puts "Address: #{msg.address}"
    puts "Subject: #{msg.subject}"
    puts "Content: #{msg.content}"
    puts "Message ID: #{msg.id}"
  end

messenger.stop

