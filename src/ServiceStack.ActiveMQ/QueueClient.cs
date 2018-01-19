using ServiceStack.Text;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace ServiceStack.ActiveMq
{
	internal class QueueClient : Producer, ServiceStack.Messaging.IMessageQueueClient
	{
		//private string QueueNames
		internal QueueClient(MessageFactory messageFactory) : base(messageFactory)
		{

		}

		public async Task StartAsync(Messaging.IMessageHandler handler, Func<bool> DoNext = null)
		{
			base.msgHandler = handler;
			if (DoNext == null) DoNext = new Func<bool>(() => true);
			await Task.Factory.StartNew(async () => { await this.OpenSessionAsync(); }, cancellationTokenSource.Token, TaskCreationOptions.LongRunning, TaskScheduler.Default);
			base.msgHandler.ProcessQueue(this, base.ResolveQueueNameFn(handler.MessageType.Name, ".inq"), DoNext);
		}

		public virtual void Ack(Messaging.IMessage message)
		{
			//message.Acknowledge();
		}

		public virtual void Nak(Messaging.IMessage message, bool requeue, Exception exception = null)
		{

		}

		public ServiceStack.Messaging.IMessage<T> CreateMessage<T>(object mqResponse)
		{
			return ((Apache.NMS.IObjectMessage)mqResponse).ToMessage<T>();
		}

		public ServiceStack.Messaging.IMessage<T> Get<T>(string queueName, TimeSpan? timeSpanOut = null)
		{
			/// Manage timeout in that function
			timeSpanOut = timeSpanOut.HasValue ? timeSpanOut.Value : Timeout.InfiniteTimeSpan;
			ServiceStack.Messaging.IMessage<T> message = null;
			Task task = Task.Factory.StartNew(() => { message = this.GetAsync<T>(queueName); });
			bool success = task.Wait(timeSpanOut.Value);
			if (success)
			{
				this.msgHandler.ProcessMessage(this, ((Apache.NMS.IObjectMessage)message));
			}
			return message;
		}


		/// <summary>
		/// This method should be called asynchronously
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="queueName"></param>
		/// <returns></returns>
		public ServiceStack.Messaging.IMessage<T> GetAsync<T>(string queueName)
		{
			using (Apache.NMS.IMessageConsumer consumer = this.GetConsumer(queueName).Result)
			{
				ServiceStack.Messaging.IMessage<T> response = null;
				Apache.NMS.MessageListener listener = new Apache.NMS.MessageListener((Apache.NMS.IMessage message) =>
				{
					// After ConsumerTransform, message is a valid Apache.NMS.IObjectMessage
					var msg = message as Apache.NMS.IObjectMessage;
					try
					{
						msg.Body = ServiceStack.Text.JsonSerializer.DeserializeFromString(msg.Body.ToString(), this.msgHandler.MessageType);
						response = CreateMessage<T>(msg);
						GetMessageFilter?.Invoke(queueName, response);
					}
					catch (Exception ex)
					{
						ex.Data.Add("Message", message.ToString());
						InvalidCastException Exception = new InvalidCastException($"The message could not be deserialized as a valid [{this.msgHandler.MessageType.Name}] message", ex);
						this.OnMessagingError(Exception);
					}
				});
				consumer.Listener += listener;
				Log.Debug($"Message of type [{typeof(T)}] (InQ) are retrieved from queue: [{queueName}]");
				while (!this.cancellationTokenSource.IsCancellationRequested && response == null)
				{
					Task.Delay(500);
				}
				consumer.Listener -= listener;
				return response;
			}
		}

		public void Notify(string queueName, ServiceStack.Messaging.IMessage message)
		{
			throw new NotImplementedException();
		}

	}
}