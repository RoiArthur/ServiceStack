using Apache.NMS;
using ServiceStack.Logging;
using System;
using System.Collections.Generic;


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

		internal MessageFactory(IConnectionFactory connectionFactory)
		{
			System.Diagnostics.Contracts.Contract.Requires(connectionFactory != null && connectionFactory.BrokerUri != null);
			if (connectionFactory == null)
				throw new ArgumentNullException(nameof(connectionFactory));

			try
			{
				this.ConnectionFactory = connectionFactory;
				string prefix = $"{Environment.MachineName}-{System.Diagnostics.Process.GetCurrentProcess().ProcessName}";
				this.BrokerUri = connectionFactory.BrokerUri;

				dynamic transport = null;
				if (this.TransportType == ConnectionType.ActiveMQ)
				{
					this.GenerateConnectionId = new Func<string>((new Apache.NMS.ActiveMQ.Util.IdGenerator(prefix)).GenerateSanitizedId);
					transport = Apache.NMS.ActiveMQ.Transport.TransportFactory.CreateTransport(this.BrokerUri);
				}

				if (this.TransportType == ConnectionType.STOMP)
				{
					this.GenerateConnectionId = new Func<string>((new Apache.NMS.Stomp.Util.IdGenerator(prefix)).GenerateSanitizedId);
					transport = Apache.NMS.Stomp.Transport.TransportFactory.CreateTransport(this.BrokerUri);
				}

				this.isConnected = new Func<bool>(() => transport.IsConnected);
				this.isFaultTolerant = new Func<bool>(() => transport.IsFaultTolerant);
				this.isStarted = new Func<bool>(() => transport.IsStarted);
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
				if (((Apache.NMS.NMSConnectionFactory)ConnectionFactory).ConnectionFactory is Apache.NMS.ActiveMQ.ConnectionFactory) return ConnectionType.ActiveMQ;
				if (((Apache.NMS.NMSConnectionFactory)ConnectionFactory).ConnectionFactory is Apache.NMS.Stomp.ConnectionFactory) return ConnectionType.STOMP;
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
			return new QueueClient(this)
			{
				PublishMessageFilter = PublishMessageFilter,
				GetMessageFilter = GetMessageFilter,
				ResolveQueueNameFn = ResolveQueueNameFn
			};

	}

	public virtual Messaging.IMessageProducer CreateMessageProducer()
	{
		return new Producer(this)
		{
			PublishMessageFilter = PublishMessageFilter,
			GetMessageFilter = GetMessageFilter,
			ResolveQueueNameFn = ResolveQueueNameFn
		};
	}

	public void Dispose()
	{
			this.ConnectionFactory = null;
	}
}
}