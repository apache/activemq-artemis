/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Threading;
using Amqp.Framing;
using Amqp;
using System.Threading.Tasks;

namespace Artemis.Perf
{

    // this has been copied from Artemis' TokenBucketLimiter with some modifications
    public class TokenBucketLimiterImpl {

        private int rate;

        /**
            * Even thought we don't use TokenBucket in multiThread
            * the implementation should keep this volatile for correctness
            */
        private long last;

        /**
            * Even thought we don't use TokenBucket in multiThread
            * the implementation should keep this volatile for correctness
            */
        private int tokens;

            public TokenBucketLimiterImpl(int rate) {
            this.rate = rate;
        }
        private bool checkRate() {

            long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            if (last == 0) {
                last = now;
            }

            long diff = now - last;

            if (diff >= 1000) {
                last = now;

                tokens = rate;
            }

            if (tokens > 0) {
                tokens--;

                return true;
            } else {
                return false;
            }
        }

        public void limit() {
            if (!checkRate()) {
                // Console.WriteLine("Limiting messages per max rate");
                do {
                    Thread.Sleep(1);
                } while (!checkRate());
            }
        }
    }
}