using ServiceStack.Logging;
using System;
using System.Threading.Tasks;

namespace ServiceStack.ActiveMq
{
	internal class Worker
	{
		private static readonly ILog Log = LogManager.GetLogger(typeof(Worker));

		internal readonly ServiceStack.Messaging.IMessageFactory messageFactory;
		internal readonly ServiceStack.Messaging.IMessageHandler messageHandler;

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

		private Worker(ServiceStack.Messaging.IMessageFactory factory,
			ServiceStack.Messaging.IMessageHandler handler,
			Action<Worker, Exception> errorHandler)
		{
			this.messageFactory = factory;
			this.messageHandler = handler;
			this.ErrorHandler = errorHandler;
		}

		internal async Task Subscribe(ServiceStack.Messaging.IMessageHandler handler, int messagesCount = int.MaxValue, TimeSpan? timeOut = null)
		{
			try
			{
				await ((QueueClient)this.MQClient).StartAsync(handler);
			}
			catch (Exception ex)
			{
				ErrorHandler?.Invoke(this, ex);
				Log.Error("Could not START Active MQ Worker : ", ex);
			}

		}

		internal static async Task<Worker> StartAsync(Server service, ServiceStack.Messaging.IMessageHandlerFactory factory)
		{
			ServiceStack.Messaging.IMessageHandler handler = factory.CreateMessageHandler();
			Worker client = new Worker(service.MessageFactory, handler, service.ErrorHandler);
			await Task.Factory.StartNew(() => client.Subscribe(handler));
			return client;
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
	

