#!/usr/bin/env ruby
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

require 'qpid_proton'

messenger = Qpid::Proton::Messenger.new()
messenger.incoming_window = 10
messenger.timeout = 10000

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
    messenger.receive(1)
  rescue Qpid::Proton::ProtonError => error
    puts "ERROR: #{error.message}"
    exit
  end

  while messenger.incoming.nonzero?
    begin
      messenger.get(msg)
      # for 0.5:
      # messenger.accept()

      # for 0.4:
      #messenger.accept(messenger.incoming_tracker, 0)#1 would mean cumulative

      # optional and the same in both versions (messenger will
      # settle itself when tracker passes out the window)
      # messenger.settle(messenger.incoming_tracker)


    rescue Qpid::Proton::Error => error
      puts "ERROR: #{error.message}"
      exit
    end

    puts "Address: #{msg.address}"
    puts "Subject: #{msg.subject}"
    puts "Content: #{msg.body}"
    puts "Message ID: #{msg.id}"
  end

messenger.stop

