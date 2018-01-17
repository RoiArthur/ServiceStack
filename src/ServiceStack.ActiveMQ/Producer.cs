using ServiceStack.Logging;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace ServiceStack.ActiveMq
{
	internal partial class Producer : ServiceStack.Messaging.IMessageProducer, IOneWayClient
	{
		public static ILog Log = LogManager.GetLogger(typeof(Producer));
		internal const string MetaOriginMessage = "QueueMessage";

		protected readonly MessageFactory msgFactory;

		protected Messaging.IMessageHandler msgHandler;

		public Func<object, string, string> ResolveQueueNameFn { get; internal set; }

		public bool IsReceiver
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
				return !this.IsReceiver; // Producer
			}
		}

		public Action OnPublishedCallback { get; set; }

		public Action<string, Apache.NMS.IPrimitiveMap, ServiceStack.Messaging.IMessage> PublishMessageFilter { get; set; }
		public Action<string, ServiceStack.Messaging.IMessage> GetMessageFilter { get; set; }

		internal Producer(MessageFactory factory)
		{
			semaphoreConsumer = new System.Threading.SemaphoreSlim(1);
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
			await Task<bool>.Factory.StartNew(() =>
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
						return false;
					}
					catch (Exception ex)
					{
						ex = new Apache.NMS.MessageFormatException($"Unable to send message of type {message.Body.GetType().Name}", ex.GetBaseException());
						this.OnMessagingError(ex);
						return false;
					}

					this.State = System.Data.ConnectionState.Fetching;
				}
					
				return true;
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