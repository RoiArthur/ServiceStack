﻿using Apache.NMS;
using ServiceStack.Logging;
using ServiceStack.Messaging;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace ServiceStack.ActiveMq
{
	public class MessageFactory : ServiceStack.Messaging.IMessageFactory
	{
		private static readonly ILog Log = LogManager.GetLogger(typeof(MessageFactory));

		internal Apache.NMS.IConnectionFactory ConnectionFactory = null;

		public string UserName { get; private set; }
		internal string Password { get; private set; }

		public Func<object, string, string> ResolveQueueNameFn { get; internal set; }
		public Action<string, Apache.NMS.IPrimitiveMap, ServiceStack.Messaging.IMessage> PublishMessageFilter { get; set; }
		public Action<string, ServiceStack.Messaging.IMessage> GetMessageFilter { get; set; }

		internal MessageFactory(string BrokerUri, string Username, string Password) :
			this(new Uri(BrokerUri), Username, Password)
		{

		}

		internal MessageFactory(Uri BrokerUri, string Username, string Password)
		{
			IConnectionFactory connectionFactory = Apache.NMS.NMSConnectionFactory.CreateConnectionFactory(BrokerUri);
			this.UserName = UserName;
			this.Password = Password;
			BuildConnectionFactory(connectionFactory);
		}

		internal MessageFactory(IConnectionFactory connectionFactory)
		{
			BuildConnectionFactory(connectionFactory);
		}

		private void BuildConnectionFactory(IConnectionFactory connectionFactory)
		{
			System.Diagnostics.Contracts.Contract.Requires(connectionFactory != null && connectionFactory.BrokerUri != null);
			if (connectionFactory == null)
				throw new ArgumentNullException(nameof(connectionFactory));

			try
			{
				this.Logger().Debug($"Creates connection [{this.TransportType}] from Activeq broker [{this.BrokerUri}]");

				this.ConnectionFactory = connectionFactory;
				string prefix = $"{System.Diagnostics.Process.GetCurrentProcess().ProcessName}-{Environment.MachineName}";
				this.BrokerUri = connectionFactory.BrokerUri;

				dynamic transport = null;
				if (this.TransportType == ConnectionType.ActiveMQ)
				{
					this.GenerateConnectionId = new Func<string>((new Apache.NMS.ActiveMQ.Util.IdGenerator(prefix)).GenerateSanitizedId);
					transport = Apache.NMS.ActiveMQ.Transport.TransportFactory.CreateTransport(this.BrokerUri);
					((Apache.NMS.ActiveMQ.ConnectionFactory)this.ConnectionFactory).UserName = this.UserName;
					((Apache.NMS.ActiveMQ.ConnectionFactory)this.ConnectionFactory).Password = this.Password;
				}

				if (this.TransportType == ConnectionType.STOMP)
				{
					this.GenerateConnectionId = new Func<string>((new Apache.NMS.Stomp.Util.IdGenerator(prefix)).GenerateSanitizedId);
					transport = Apache.NMS.Stomp.Transport.TransportFactory.CreateTransport(this.BrokerUri);
					((Apache.NMS.Stomp.ConnectionFactory)this.ConnectionFactory).UserName = this.UserName;
					((Apache.NMS.Stomp.ConnectionFactory)this.ConnectionFactory).Password = this.Password;
				}

				this.isConnected = new Func<bool>(() => transport.IsConnected);
				this.isFaultTolerant = new Func<bool>(() => transport.IsFaultTolerant);
				this.isStarted = new Func<bool>(() => transport.IsStarted);

				string state = transport.IsConnected ? "Connected" : "NotConnected";
				this.Logger().Info($"Connection state [{state}]");
			}
			catch (Exception ex)
			{
				List<string> detailledError = new List<string>() {  $"Unable to connect ActiveMQ Broker using :",
																		$"   - [ConnectionString : {this.ConnectionFactory.BrokerUri}]",
																		//$"   - [Username : {this.connectionFactory.UserName}]" ,
																		$"   - [Error : {ex.GetBaseException().Message}]" };
				throw new InvalidOperationException(string.Join(Environment.NewLine, detailledError.ToArray()), ex.GetBaseException());
			}
		}

		internal async Task<IConnection> GetConnectionAsync()
		{
			IConnection connection = null;
			Exception ex = null;
			bool retry = false;
			try
			{
				Log.Info($"Etablish connection to ActiveMQBroker {this.ConnectionFactory.BrokerUri}");
				connection = this.ConnectionFactory.CreateConnection(this.UserName, this.Password);
				connection.ClientId = this.GenerateConnectionId();
				return connection;
			}
			catch (NMSConnectionException exc)
			{
				ex = exc;
				retry = true;
			}
			catch (InvalidClientIDException exc)
			{
				ex = exc;
			}
			catch (Exception exc)
			{
				if (exc.Source != "Apache.NMS.Stomp") // Somme methods called by Apache.NMS to Stomp are not implemented... in Apache.NMS.Stomp
				{
					ex = exc;
					throw;
				}
				else
				{
					Log.Warn(exc.Message);
				}
			}

			if (retry)
			{
				Log.Warn($"[Worker {connection.ClientId}] > {ex.Message} - Retry in 5 seconds");
				new System.Threading.AutoResetEvent(false).WaitOne(NMSConstants.defaultRequestTimeout);
				return await GetConnectionAsync();            // Retry
			}
			else
			{
				Log.Error($"Could not connect to ActiveMQ [{this.ConnectionFactory.BrokerUri}]", ex.GetBaseException());
			}
			return null;
		}

		public enum ConnectionType
		{
			ActiveMQ,
			STOMP,
			MSMQ,
			EMS,
			WCF,
			AMQP,
			MQTT,
			XMS
		}

		public ConnectionType TransportType
		{
			get
			{
				if (ConnectionFactory is Apache.NMS.ActiveMQ.ConnectionFactory) return ConnectionType.ActiveMQ;
				if (ConnectionFactory is Apache.NMS.Stomp.ConnectionFactory) return ConnectionType.STOMP;
				return ConnectionType.ActiveMQ;
			}
		}

		public Func<string> GenerateConnectionId { get; set; }

		public Uri BrokerUri { get; private set; }

		public Func<bool> isConnected { get; private set; }

		public Func<bool> isFaultTolerant { get; private set; }

		public Func<bool> isStarted { get; private set; }

		public virtual Messaging.IMessageQueueClient CreateMessageQueueClient()
		{
			this.Logger().Debug($"Creates Queue Consumer");
			return new QueueClient(this)
			{
				PublishMessageFilter = PublishMessageFilter,
				GetMessageFilter = GetMessageFilter,
				ResolveQueueNameFn = ResolveQueueNameFn
			};
		}

		public virtual Messaging.IMessageProducer CreateMessageProducer()
		{
			this.Logger().Debug($"Creates Queue Publisher");
			return new Producer(this)
			{
				PublishMessageFilter = PublishMessageFilter,
				GetMessageFilter = GetMessageFilter,
				ResolveQueueNameFn = ResolveQueueNameFn
			};
		}

		#region IDisposable Support
		private bool disposedValue = false; // To detect redundant calls

		protected virtual void Dispose(bool disposing)
		{
			if (!disposedValue)
			{
				this.Logger().Debug($"Dispose Message Factory");
				if (disposing)
				{
					// Close Listening Thread
					this.ConnectionFactory = null;
				}
				// TODO: free unmanaged resources (unmanaged objects) and override a finalizer below.
				// TODO: set large fields to null.
				disposedValue = true;
			}
		}

		// TODO: override a finalizer only if Dispose(bool disposing) above has code to free unmanaged resources.
		// ~Producer() {
		//   // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
		//   Dispose(false);
		// }

		// This code added to correctly implement the disposable pattern.
		public void Dispose()
		{
			// Do not change this code. Put cleanup code in Dispose(bool disposing) above.
			Dispose(true);
			// TODO: uncomment the following line if the finalizer is overridden above.
			GC.SuppressFinalize(this);
			GC.Collect();
		}

		public void Add(IMessageHandlerStats stats)
		{
			throw new NotImplementedException();
		}
		#endregion

	}
}