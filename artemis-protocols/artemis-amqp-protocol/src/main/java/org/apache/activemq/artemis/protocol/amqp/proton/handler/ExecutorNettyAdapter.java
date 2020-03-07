/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.apache.activemq.artemis.protocol.amqp.proton.handler;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ProgressivePromise;
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.ScheduledFuture;
import org.apache.activemq.artemis.utils.actors.ArtemisExecutor;

/** Test cases may supply a simple executor instead of the real Netty Executor
 *  On that case this is a simple adapter for what's needed from these tests.
 *  Not intended to be used in production.
 *
 *  TODO: This could be refactored out of the main codebase but at a high cost.
 *        We may do it some day if we find an easy way that won't clutter the code too much.
 *  */
public class ExecutorNettyAdapter implements EventLoop {

   final ArtemisExecutor executor;

   public ExecutorNettyAdapter(ArtemisExecutor executor) {
      this.executor = executor;
   }

   @Override
   public EventLoopGroup parent() {
      return null;
   }

   @Override
   public EventLoop next() {
      return null;
   }

   @Override
   public ChannelFuture register(Channel channel) {
      return null;
   }

   @Override
   public ChannelFuture register(ChannelPromise promise) {
      return null;
   }

   @Override
   public ChannelFuture register(Channel channel, ChannelPromise promise) {
      return null;
   }

   @Override
   public boolean inEventLoop() {
      return inEventLoop(Thread.currentThread());
   }

   @Override
   public boolean inEventLoop(Thread thread) {
      return false;
   }

   @Override
   public <V> Promise<V> newPromise() {
      return null;
   }

   @Override
   public <V> ProgressivePromise<V> newProgressivePromise() {
      return null;
   }

   @Override
   public <V> Future<V> newSucceededFuture(V result) {
      return null;
   }

   @Override
   public <V> Future<V> newFailedFuture(Throwable cause) {
      return null;
   }

   @Override
   public boolean isShuttingDown() {
      return false;
   }

   @Override
   public Future<?> shutdownGracefully() {
      return null;
   }

   @Override
   public Future<?> shutdownGracefully(long quietPeriod, long timeout, TimeUnit unit) {
      return null;
   }

   @Override
   public Future<?> terminationFuture() {
      return null;
   }

   @Override
   public void shutdown() {

   }

   @Override
   public List<Runnable> shutdownNow() {
      return null;
   }

   @Override
   public Iterator<EventExecutor> iterator() {
      return null;
   }

   @Override
   public Future<?> submit(Runnable task) {
      execute(task);
      return null;
   }

   @Override
   public <T> Future<T> submit(Runnable task, T result) {
      execute(task);
      return null;
   }

   @Override
   public <T> Future<T> submit(Callable<T> task) {
      return null;
   }

   @Override
   public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
      return null;
   }

   @Override
   public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
      return null;
   }

   @Override
   public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
      return null;
   }

   @Override
   public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
      return null;
   }

   @Override
   public boolean isShutdown() {
      return false;
   }

   @Override
   public boolean isTerminated() {
      return false;
   }

   @Override
   public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
      return false;
   }

   @Override
   public <T> List<java.util.concurrent.Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
      return null;
   }

   @Override
   public <T> List<java.util.concurrent.Future<T>> invokeAll(Collection<? extends Callable<T>> tasks,
                                                             long timeout,
                                                             TimeUnit unit) throws InterruptedException {
      return null;
   }

   @Override
   public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
      return null;
   }

   @Override
   public <T> T invokeAny(Collection<? extends Callable<T>> tasks,
                          long timeout,
                          TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
      return null;
   }

   @Override
   public void execute(Runnable command) {
      executor.execute(command);
   }
}
