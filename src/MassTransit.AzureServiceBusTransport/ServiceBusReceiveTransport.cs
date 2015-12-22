﻿// Copyright 2007-2015 Chris Patterson, Dru Sellers, Travis Smith, et. al.
//  
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the 
// License at 
// 
//     http://www.apache.org/licenses/LICENSE-2.0 
// 
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR 
// CONDITIONS OF ANY KIND, either express or implied. See the License for the 
// specific language governing permissions and limitations under the License.
namespace MassTransit.AzureServiceBusTransport
{
    using System;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Configuration;
    using Contexts;
    using Logging;
    using MassTransit.Pipeline;
    using Pipeline;
    using Policies;
    using Transports;
    using Util;


    public class ServiceBusReceiveTransport :
        IReceiveTransport
    {
        static readonly ILog _log = Logger.Get<ServiceBusReceiveTransport>();
        readonly ReceiveEndpointObservable _endpointObservers;
        readonly IServiceBusHost _host;
        readonly ReceiveObservable _receiveObservers;
        readonly ReceiveSettings _settings;
        readonly TopicSubscriptionSettings[] _subscriptionSettings;
        readonly IRetryPolicy _connectionRetryPolicy;

        public ServiceBusReceiveTransport(IServiceBusHost host, ReceiveSettings settings,
            params TopicSubscriptionSettings[] subscriptionSettings)
        {
            _host = host;
            _settings = settings;
            _subscriptionSettings = subscriptionSettings;
            _receiveObservers = new ReceiveObservable();
            _endpointObservers = new ReceiveEndpointObservable();

            _connectionRetryPolicy = Retry.Exponential(1000, TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(1));
        }

        void IProbeSite.Probe(ProbeContext context)
        {
            var scope = context.CreateScope("transport");
            scope.Set(new
            {
                Type = "Azure Service Bus",
                _settings.QueueDescription.Path,
                _settings.PrefetchCount,
                _settings.MaxConcurrentCalls,
                _settings.QueueDescription,
                Subscriptions = _subscriptionSettings.Select(subscription => new
                {
                    subscription.Topic.Path
                }).ToArray()
            });
        }

        public ReceiveTransportHandle Start(IPipe<ReceiveContext> receivePipe)
        {
            if (_log.IsDebugEnabled)
                _log.DebugFormat("Starting receive transport: {0}", new Uri(_host.Settings.ServiceUri, _settings.QueueDescription.Path));

            var supervisor = new TaskSupervisor();

            var connectionPipe = Pipe.New<ConnectionContext>(x =>
            {
                x.UseFilter(new PrepareReceiveQueueFilter(_settings, _subscriptionSettings));

                if (_settings.QueueDescription.RequiresSession)
                {
                    x.UseFilter(new MessageSessionReceiverFilter(receivePipe, _receiveObservers, _endpointObservers, supervisor));
                }
                else
                {
                    x.UseFilter(new MessageReceiverFilter(receivePipe, _receiveObservers, _endpointObservers, supervisor));
                }
            });

            Receiver(supervisor, connectionPipe);

            return new Handle(supervisor);
        }

        public ConnectHandle ConnectReceiveObserver(IReceiveObserver observer)
        {
            return _receiveObservers.Connect(observer);
        }

        public ConnectHandle ConnectReceiveEndpointObserver(IReceiveEndpointObserver observer)
        {
            return _endpointObservers.Connect(observer);
        }

        async void Receiver(TaskSupervisor supervisor, IPipe<ConnectionContext> connectionPipe)
        {
            await _connectionRetryPolicy.RetryUntilCancelled(async () =>
            {
                if (_log.IsDebugEnabled)
                    _log.DebugFormat("Connecting receive transport: {0}", _host.Settings.GetInputAddress(_settings.QueueDescription));

                var context = new ServiceBusConnectionContext(_host, supervisor.StopToken);

                try
                {
                    await connectionPipe.Send(context).ConfigureAwait(false);
                }
                catch (TaskCanceledException)
                {
                }
                catch (Exception ex)
                {
                    if (_log.IsErrorEnabled)
                        _log.ErrorFormat("Azure Service Bus connection failed: {0}", ex.Message);

                    var inputAddress = context.GetQueueAddress(_settings.QueueDescription);

                    await _endpointObservers.Faulted(new Faulted(inputAddress, ex)).ConfigureAwait(false);
                }
            }, supervisor.StoppingToken).ConfigureAwait(false);
        }


        class Faulted :
            ReceiveEndpointFaulted
        {
            public Faulted(Uri inputAddress, Exception exception)
            {
                InputAddress = inputAddress;
                Exception = exception;
            }

            public Uri InputAddress { get; }
            public Exception Exception { get; }
        }


        class Handle :
            ReceiveTransportHandle
        {
            readonly TaskSupervisor _supervisor;

            public Handle(TaskSupervisor supervisor)
            {
                _supervisor = supervisor;
            }

            async Task ReceiveTransportHandle.Stop(CancellationToken cancellationToken)
            {
                await _supervisor.Stop("Receive Transport Stopping").ConfigureAwait(false);

                await _supervisor.Completed.ConfigureAwait(false);
            }
        }
    }
}