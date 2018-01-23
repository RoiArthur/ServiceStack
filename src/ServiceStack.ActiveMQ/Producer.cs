using ServiceStack.Logging;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace ServiceStack.ActiveMq
{
	internal partial class Producer : ServiceStack.Messaging.IMessageProducer, IOneWayClient
	{
		public static ILog Log = LogManager.GetLogger(typeof(Producer));
		internal const string MetaOriginMessage = "QueueMessage";

		internal Messaging.IMessageFactory MessageFactory { get; set; }

		internal Messaging.IMessageHandler MessageHandler { get; set; }

		public Func<object, string, string> ResolveQueueNameFn { get; internal set; }

		public bool IsConsumer
		{
			get
			{
				return this.GetType().IsSubclassOf(typeof(Producer)); // QueueClient
			}
		}

		public bool IsEmitter
		{
			get
			{
				return !this.IsConsumer; // Producer
			}
		}

		public Action OnPublishedCallback { get; set; }

		public Action<string, Apache.NMS.IPrimitiveMap, ServiceStack.Messaging.IMessage> PublishMessageFilter { get; set; }
		public Action<string, ServiceStack.Messaging.IMessage> GetMessageFilter { get; set; }

		internal Producer()
		{
			semaphoreProducer = new System.Threading.SemaphoreSlim(1);
			//this.MessageHandler = handlerFactory.CreateMessageHandler();
			//this.MessageFactory = factory;
			this.ConnectionName = "Not connected";
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
			var message = messageBody as ServiceStack.Messaging.Message<T>;
			if (message != null)
			{
				Publish((ServiceStack.Messaging.Message<T>)message);
			}
			else
			{
				Publish(new ServiceStack.Messaging.Message<T>(messageBody));
			}
		}

		public void Publish<T>(ServiceStack.Messaging.IMessage<T> message)
		{
			Publish(null, message);
		}

		public void Publish<T>(ServiceStack.Messaging.Message<T> message)
		{
			Publish(null, message);
		}

		public virtual void Publish(string queueName, ServiceStack.Messaging.IMessage message)
		{
			if (this.cancellationTokenSource.IsCancellationRequested) return;
			if (string.IsNullOrWhiteSpace(queueName)) queueName = this.ResolveQueueNameFn(message.Body, ".outq");
			Publish(queueName, message, Messaging.QueueNames.Exchange);
		}

		protected AutoResetEvent publishing = new AutoResetEvent(true);
		private async void Publish(string queueName, ServiceStack.Messaging.IMessage message, string topic)
		{
			await Task.Factory.StartNew(() =>
			{
				
				using (Apache.NMS.IMessageProducer producer = this.GetProducer(queueName).Result)
				{
					
					this.State = System.Data.ConnectionState.Executing;

					Apache.NMS.IObjectMessage apacheMessage = producer.CreateMessage(message);
					PublishMessageFilter?.Invoke(queueName, apacheMessage.Properties, message);

					apacheMessage.Body = message.Body;
					try
					{
						producer.Send(apacheMessage);
						OnPublishedCallback?.Invoke();
					}
					catch (Apache.NMS.NMSException ex)
					{
						ex = new Apache.NMS.NMSException($"Unable to send message of type {message.Body.GetType().Name}", ex);
						this.OnTransportError(ex);
						throw ex;
					}
					catch (Exception ex)
					{
						ex = new Apache.NMS.MessageFormatException($"Unable to send message of type {message.Body.GetType().Name}", ex.GetBaseException());
						this.OnMessagingError(ex);
						throw ex;
					}

					this.State = System.Data.ConnectionState.Fetching;
				}
			});
		}

		public virtual void SendOneWay(object requestDto)
		{
			Publish(Messaging.MessageFactory.Create(requestDto));
		}

		public virtual void SendOneWay(string queueName, object requestDto)
		{
			Publish(queueName, Messaging.MessageFactory.Create(requestDto));
		}

		public virtual void SendAllOneWay(IEnumerable<object> requests)
		{
			if (requests == null) return;
			foreach (var request in requests)
			{
				SendOneWay(request);
			}
		}

	}
}