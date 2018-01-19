using ServiceStack.Logging;
using System;
using System.Threading.Tasks;

namespace ServiceStack.ActiveMq
{
	internal class Worker
	{
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
				}
				return client;
			}
		}

		private Worker(ServiceStack.Messaging.IMessageFactory messageFactory, ServiceStack.Messaging.IMessageHandlerFactory handlerFactory, Action<Worker, Exception> errorHandler)
		{
			this.messageFactory = messageFactory;
			this.messageHandlerFactory = handlerFactory;
			this.ErrorHandler = errorHandler;
		}

		private static async Task<Worker> CreateWorkerAsync(Server service, ServiceStack.Messaging.IMessageHandlerFactory handlerFactory)
		{
			Worker client = new Worker(service.MessageFactory, handlerFactory, service.ErrorHandler);
			await Task.Factory.StartNew(() => client.Subscribe());
			return client;
		}

		internal async Task Subscribe(int messagesCount = int.MaxValue, TimeSpan? timeOut = null)
		{
			try
			{
				await ((QueueClient)this.MQClient).StartAsync(this.messageHandlerFactory, ()=> messagesCount == int.MaxValue && !timeOut.HasValue);
			}
			catch (Exception ex)
			{
				ErrorHandler?.Invoke(this, ex);
				Log.Error("Could not START Active MQ Worker : ", ex);
			}

		}

		internal static async Task<Worker> StartAsync(Server service, ServiceStack.Messaging.IMessageHandlerFactory factory)
		{
			return await CreateWorkerAsync(service,factory);
		}

		internal async Task CloseAsync()
		{
			await Task.Factory.StartNew(() => this.Dispose());
		}
		#region IDisposable Members

		private bool isDisposed = false;
		public void Dispose()
		{
			if (!this.isDisposed)
			{
				this.MQClient.Dispose();
				this.isDisposed = true;
			}
		}

		#endregion

	}
}
	

