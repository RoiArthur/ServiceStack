using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Apache.NMS;
using ServiceStack.Logging;
using ServiceStack.Text;

namespace ServiceStack.ActiveMq
{
    internal class Producer : ServiceStack.Messaging.IMessageProducer, IDisposable//, IOneWayClient
    {
        public static ILog Log = LogManager.GetLogger(typeof(Producer));
		internal const string MetaOriginMessage = "QueueMessage";

		protected readonly MessageFactory msgFactory;

		protected Messaging.IMessageHandler msgHandler;

		public Func<object, string, string> ResolveQueueNameFn { get; internal set; }

		public bool IsReceiver {
			get
			{
				return this.GetType().IsSubclassOf(typeof(Producer)) ; // QueueClient
			}
		}

		public bool IsEmitter
		{
			get
			{
				return !this.IsReceiver; // Producer
			}
		}

		public event EventHandler<System.Data.ConnectionState> ConnectionStateChanged;

		protected Apache.NMS.DestinationType QueueType = Apache.NMS.DestinationType.Queue;

		private System.Data.ConnectionState _state = System.Data.ConnectionState.Closed;
		public System.Data.ConnectionState State
		{
			get
			{
				return _state;
			}
			internal set
			{
				bool raise = _state != value;
				System.Data.ConnectionState oldstate = _state;
				_state = value;
				if (raise)
				{
					Log.Debug($"Active MQ Connector [{this.Connection.ClientId}] has changed from [{oldstate.ToString()}] to [{_state.ToString()}]");
					if (ConnectionStateChanged != null) ConnectionStateChanged(this, value);
				}
			}
		}

		public Apache.NMS.ISession Session { get; internal set; }

		private IConnection connection;
		public IConnection Connection
		{
			get
			{
				if (connection == null)
				{
					connection = msgFactory.GetConnectionAsync().Result;
				}
				return connection;
			}
		}

		internal CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();

		public Action OnPublishedCallback { get; set; }

		public Action<string, Apache.NMS.IPrimitiveMap, ServiceStack.Messaging.IMessage> PublishMessageFilter { get; set; }
		public Action<string, ServiceStack.Messaging.IMessage> GetMessageFilter { get; set; }

		private IMessage ToActiveMQ(ISession session, IMessageProducer producer, IMessage message)
		{
			IObjectMessage obj = message as IObjectMessage;
			obj.Body = JsonSerializer.SerializeToString(obj.Body);
			return obj;
		}

		internal Producer(MessageFactory factory)
		{
			this.msgFactory = factory;
		}

		internal void OnMessagingError(Exception ex)
		{
			Log.Error(ex);
		}

		internal void OnTransportError(Exception ex)
		{
			Log.Error(ex);
		}

		public virtual string GetTempQueueName()
		{
			return this.Session.CreateTemporaryQueue().QueueName;
		}

		public void Publish<T>(T messageBody)
		{
			var message = messageBody as ServiceStack.Messaging.IMessage;
			if (message != null)
			{
				Publish(message.ToInQueueName(), message);
			}
			else
			{
				Publish(new ServiceStack.Messaging.Message<T>(messageBody));
			}
		}

		public void Publish<T>(ServiceStack.Messaging.IMessage<T> message)
		{
			Publish(message.ToInQueueName(), message);
		}

		public virtual void Publish(string queueName, ServiceStack.Messaging.IMessage message)
		{
			if (string.IsNullOrWhiteSpace(queueName)) queueName = this.ResolveQueueNameFn(this.msgHandler.MessageType.Name, ".outq");
			Publish(queueName, message, Messaging.QueueNames.Exchange);
		}

		private async void Publish(string queueName, ServiceStack.Messaging.IMessage message, string topic)
		{
			IDestination destination = Session.GetDestination(queueName);
			await Task<bool>.Factory.StartNew(() => {
				using (IMessageProducer producer = Session.CreateProducer(destination))
				{
					producer.ProducerTransformer = ToActiveMQ;
					this.State = System.Data.ConnectionState.Executing;

					IObjectMessage apacheMessage= producer.CreateMessage(message);
					PublishMessageFilter?.Invoke(queueName, apacheMessage.Properties, message);

					apacheMessage.Body = message.Body;
					try
					{
						producer.Send(apacheMessage);
					}
					catch (NMSException ex)
					{
						ex = new Apache.NMS.NMSException($"Unable to send message of type {message.Body.GetType().Name}", ex);
						throw ex;
					}
					catch (Exception ex)
					{
						throw new Apache.NMS.MessageFormatException($"Unable to send message of type {message.Body.GetType().Name}", ex.GetBaseException());
					}
					
					this.State = System.Data.ConnectionState.Fetching;
				}
				return true;
			});
			OnPublishedCallback?.Invoke();
		}

		public virtual void Dispose()
		{
			// Close Listening Thread
			cancellationTokenSource.Cancel();

			if (Session != null) Session.Dispose();
			this.State = System.Data.ConnectionState.Closed;
		}
	}
}