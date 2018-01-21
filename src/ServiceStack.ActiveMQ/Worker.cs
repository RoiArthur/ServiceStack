using ServiceStack.Logging;
using System;
using System.Threading.Tasks;

namespace ServiceStack.ActiveMq
{
	internal class Worker
	{
		/// <summary>
		/// Creates and start a worker
		/// </summary>
		/// <param name="service"></param>
		/// <param name="handlerFactory"></param>
		/// <returns></returns>
		internal static async Task<Worker> StartAsync(Server service, ServiceStack.Messaging.IMessageHandlerFactory handlerFactory)
		{
			Worker client = new Worker(handlerFactory, service.MessageFactory,  service.ErrorHandler);
			await client.Dequeue();
			return client;
		}

		
		private Worker(ServiceStack.Messaging.IMessageHandlerFactory handlerFactory, ServiceStack.Messaging.IMessageFactory messageFactory, Action<Worker, Exception> errorHandler)
		{
			this.messageFactory = messageFactory;
			this.messageHandlerFactory = handlerFactory;
			this.ErrorHandler = errorHandler;
		}

		private static readonly ILog Log = LogManager.GetLogger(typeof(Worker));

		internal readonly ServiceStack.Messaging.IMessageHandlerFactory messageHandlerFactory;
		internal readonly ServiceStack.Messaging.IMessageFactory messageFactory;

		internal Action<Worker, Exception> ErrorHandler { get; set; }

		ServiceStack.Messaging.IMessageQueueClient client = null;
		internal ServiceStack.Messaging.IMessageQueueClient MQClient
		{
			get
			{
				if (client == null)
				{
					client = this.messageFactory.CreateMessageQueueClient();
					((QueueClient)client).MessageFactory = messageFactory;
					((QueueClient)client).MessageHandler = messageHandlerFactory.CreateMessageHandler();
				}
				return client;
			}
		}

		internal async Task Dequeue(int messagesCount = int.MaxValue, TimeSpan? timeOut = null)
		{
			Func<bool> DoNext = () => messagesCount == int.MaxValue && !timeOut.HasValue;
			var queue = ((QueueClient)this.MQClient);
			await Task.Factory.StartNew(async () => {
				try
				{
					await queue.StartAsync();
					string queueName = queue.ResolveQueueNameFn(queue.MessageHandler.MessageType.Name, ".inq");
					if (DoNext())
					{
						queue.MessageHandler.ProcessQueue(queue, queueName, DoNext);
					}
					else
					{
						for (int i = 0; i < messagesCount; i++)
						{
							Messaging.IMessage message = null;
							queue.MessageHandler.ProcessMessage(queue, message);
						}
					}

				}
				catch (Exception ex)
				{
					ErrorHandler?.Invoke(this, ex);
					Log.Error("Could not START Active MQ Worker : ", ex);
				}
			});
		}


		#region IDisposable Members

		private bool isDisposed = false;
		public void Dispose()
		{
			if (!this.isDisposed)
			{
				this.MQClient.Dispose();
				this.messageFactory.Dispose();
				this.isDisposed = true;
			}
		}

		#endregion

	}
}
	

