﻿// Copyright 2007-2012 Chris Patterson, Dru Sellers, Travis Smith, et. al.
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
namespace MassTransit.RabbitMqTransport.Configuration.Configurators
{
    using System.Collections.Generic;
    using Builders;
    using MassTransit.Configurators;
    using RabbitMQ.Client;


    public class PasswordConnectionFactoryConfigurator :
        ConnectionFactoryBuilderConfigurator
    {
        readonly string _password;

        public PasswordConnectionFactoryConfigurator(string password)
        {
            _password = password;
        }

        public IEnumerable<ValidationResult> Validate()
        {
            if (string.IsNullOrEmpty(_password))
                yield return this.Failure("Password", "Must not be null");
        }

        ConnectionFactory Configure(ConnectionFactory connectionFactory)
        {
            connectionFactory.Password = _password;
            return connectionFactory;
        }
    }
}