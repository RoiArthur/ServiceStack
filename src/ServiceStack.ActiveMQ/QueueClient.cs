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
			semaphoreConsumer = new System.Threading.SemaphoreSlim(1);
		}


		public async Task StartAsync(Messaging.IMessageHandlerFactory handlerFactory, Func<bool> DoNext)
		{
			base.msgHandler = handlerFactory.CreateMessageHandler();
			await Task.Factory.StartNew(async () => { await this.OpenSessionAsync(); },
				cancellationTokenSource.Token,
				TaskCreationOptions.LongRunning,
				TaskScheduler.Default);

			if (DoNext())
			{
				string queueName = this.ResolveQueueNameFn(msgHandler.MessageType.Name, ".inq");
				msgHandler.ProcessQueue(this, queueName, DoNext);
			}
			else
			{
				//Apache.NMS.IObjectMessage response = new Apache.NMS.M
				//msgHandler.ProcessMessage(this, response);
			}

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
			//timeSpanOut = timeSpanOut.HasValue ? timeSpanOut.Value : Timeout.InfiniteTimeSpan;
			//ServiceStack.Messaging.IMessage<T> message = null;
			//Task task = Task.Factory.StartNew(() => { message = this.GetAsync<T>(queueName); });
			//bool success = task.Wait(timeSpanOut.Value);
			//if (success)
			//{
			//	this.msgHandler.ProcessMessage(this, ((Apache.NMS.IObjectMessage)message));
			//}
			//return message;
			return null;
		}

		static int received = 0;
		static int handled = 0;

		/// <summary>
		/// This method should be called asynchronously
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="queueName"></param>
		/// <returns></returns>
		public ServiceStack.Messaging.IMessage<T> GetAsync<T>(string queueName)
		{
			Apache.NMS.IMessageConsumer consumer = this.Consumer;
			ServiceStack.Messaging.IMessage<T> response = null;
			Log.Debug($"Message of type [{typeof(T)}] (InQ) are retrieved from queue: [{queueName}]");
			while (!this.cancellationTokenSource.IsCancellationRequested && response == null)
			{
				var msg = consumer.ReceiveNoWait() as Apache.NMS.IObjectMessage;
				if(msg!=null)
				{
					received++;
					msg.Body = ServiceStack.Text.JsonSerializer.DeserializeFromString(msg.Body.ToString(), this.msgHandler.MessageType);
					response = CreateMessage<T>(msg);
					GetMessageFilter?.Invoke(queueName, response);
				}
				Task.Delay(500);
			}
			//consumer.Listener -= listener;
			handled++;
			Console.Title = $"Messages received : {handled}/{received}";
			return response;

		}

		public void Notify(string queueName, ServiceStack.Messaging.IMessage message)
		{
			throw new NotImplementedException();
		}

	}
}