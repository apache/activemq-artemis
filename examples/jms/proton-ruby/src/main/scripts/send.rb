#!/usr/bin/env ruby

require 'qpid_proton'

address = "127.0.0.1:5672/testQueue"

messenger = Qpid::Proton::Messenger.new
messenger.start

msg = Qpid::Proton::Message.new
msg.address = address
msg.subject = "The time is #{Time.new}"
msg.content = "Hello world!"
msg.correlation_id = "554545"

begin
    messenger.put(msg)
rescue Qpid::Proton::ProtonError => error
    puts "ERROR: #{error.message}"
    exit
end

begin
    messenger.send
rescue Qpid::Proton::ProtonError => error
    puts "ERROR: #{error.message}"
    puts error.backtrace.join("\n")
    exit
end

puts "SENT: " + msg.content

messenger.stop
