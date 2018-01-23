using ServiceStack.Text;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace ServiceStack.ActiveMq
{
	internal class QueueClient : Producer, ServiceStack.Messaging.IMessageQueueClient
	{
		//private string QueueNames
		internal QueueClient()
		{
			semaphoreConsumer = new System.Threading.SemaphoreSlim(1);
		}

		public async Task StartAsync()
		{
			await Task.Factory.StartNew(async () => { await this.OpenSessionAsync(); },
				cancellationTokenSource.Token,
				TaskCreationOptions.LongRunning,
				TaskScheduler.Default);
		}

		public virtual void Ack(Messaging.IMessage message)
		{
			if(this.Session.AcknowledgementMode == Apache.NMS.AcknowledgementMode.ClientAcknowledge)
			{
				((Apache.NMS.IMessage)message.Body).Acknowledge();
			}
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
			if (timeSpanOut == null) timeSpanOut = Timeout.InfiniteTimeSpan;
			if (!this.cancellationTokenSource.IsCancellationRequested)
			{
				Func<Apache.NMS.IMessage> receiver = () => this.Consumer.Receive(timeSpanOut.Value);
				return Get<T>(queueName, receiver);
			}
			return null;
		}

		/// <summary>
		/// This method should be called asynchronously
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="queueName"></param>
		/// <returns></returns>
		public ServiceStack.Messaging.IMessage<T> GetAsync<T>(string queueName)
		{
			if(!this.cancellationTokenSource.IsCancellationRequested)
			{
				Func<Apache.NMS.IMessage> receiver = () => this.Consumer.Receive();
				return Get<T>(queueName, receiver);
			}
			return null;
		}

		public ServiceStack.Messaging.IMessage<T> Get<T>(string queueName, Func<Apache.NMS.IMessage> receiver)
		{
			ServiceStack.Messaging.IMessage<T> response = null;
			Log.Debug($"Message of type [{typeof(T)}] (InQ) are retrieved from queue: [{queueName}]");

			
			var msg = receiver() as Apache.NMS.IObjectMessage;
			if (msg != null)
			{
				response = CreateMessage<T>(msg);
				if (response!=null)
				{
					GetMessageFilter?.Invoke(queueName, response);
				}
			}
			if(response==null && !this.cancellationTokenSource.IsCancellationRequested) //Message was not an object T => Relaunch Listen
			{
				Get<T>(queueName, receiver);
			}
			while (!this.cancellationTokenSource.IsCancellationRequested && response == null)
			{
				Task.Delay(500);
			}
			return response;
		}

		public void Notify(string queueName, ServiceStack.Messaging.IMessage message)
		{
			throw new NotImplementedException();
		}

	}
}