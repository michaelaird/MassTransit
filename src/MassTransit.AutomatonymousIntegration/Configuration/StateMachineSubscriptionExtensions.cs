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
namespace Automatonymous
{
    using MassTransit;
    using MassTransit.Saga;
    using SubscriptionConfigurators;
    using SubscriptionConnectors;


    public static class StateMachineSubscriptionExtensions
    {
        /// <summary>
        /// Subscribe a state machine saga to the endpoint
        /// </summary>
        /// <typeparam name="TInstance">The state machine instance type</typeparam>
        /// <param name="configurator"></param>
        /// <param name="stateMachine">The state machine</param>
        /// <param name="repository">The saga repository for the instances</param>
        /// <returns></returns>
        public static void StateMachineSaga<TInstance>(
            this IReceiveEndpointConfigurator configurator, SagaStateMachine<TInstance> stateMachine,
            ISagaRepository<TInstance> repository)
            where TInstance : class, SagaStateMachineInstance
        {
            var stateMachineConfigurator = new StateMachineSagaSpecification<TInstance>(stateMachine, repository);

            configurator.AddEndpointSpecification(stateMachineConfigurator);
        }

        public static ConnectHandle ConnectStateMachineSaga<TInstance>(this IBus bus, SagaStateMachine<TInstance> stateMachine,
            ISagaRepository<TInstance> repository)
            where TInstance : class, SagaStateMachineInstance
        {
            var connector = new StateMachineConnector<TInstance>(stateMachine);

            return connector.ConnectSaga(bus, repository);
        }
    }
}