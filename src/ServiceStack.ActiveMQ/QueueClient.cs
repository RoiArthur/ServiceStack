using System;
using System.Threading;
using System.Threading.Tasks;
using Apache.NMS;
using ServiceStack.Messaging;
using ServiceStack.Text;

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
			string queueName = Messaging.QueueNames.ResolveQueueNameFn(this.msgHandler.MessageType.Name, ".inq");
			if(DoNext==null) DoNext = new Func<bool>(() => true);
			await Task.Factory.StartNew(async () => { await this.OpenAsync(); }, cancellationTokenSource.Token, TaskCreationOptions.LongRunning, TaskScheduler.Default);
			base.msgHandler.ProcessQueue(this, queueName, DoNext);
		}


		/// <summary>
		// Turn received Message into expected type of the ServiceStackMessageHandler<T>
		/// </summary>
		/// <param name="apsession"></param>
		/// <param name="producer"></param>
		/// <param name="message"></param>
		/// <returns></returns>
		[System.Diagnostics.DebuggerStepThrough()]
		private Apache.NMS.IObjectMessage ConsumerTransform(Apache.NMS.ISession apsession, Apache.NMS.IMessageConsumer consumer, Apache.NMS.IMessage message)
		{
			//this step ensures this message is recognized as a valid object (POCO) message (Only deserializable)
			try
			{
				Apache.NMS.IObjectMessage msg = message as Apache.NMS.IObjectMessage;
				if (msg != null)
				{
					return msg;
				}
				else
				{
					throw new Exception("Message could not be parsed as a valid Apache.NMS.IObjectMessage");
				}
			}
			catch (Exception ex)
			{
				ex.Data.Add(Producer.MetaOriginMessage, message.ToString());
				Apache.NMS.MessageNotReadableException exc = new Apache.NMS.MessageNotReadableException($"Unknown Message [{message.NMSMessageId}]: it was not a valid Json Object", ex);
				throw exc;
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

			ServiceStack.Messaging.IMessage<T> response = null;
			using (Apache.NMS.IDestination destination = Apache.NMS.Util.SessionUtil.GetDestination(this.Session, queueName))
			using (Apache.NMS.IMessageConsumer consumer = this.Session.CreateConsumer(destination))
			{
				consumer.ConsumerTransformer = this.ConsumerTransform;
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
				Log.Debug($"Message of type [{typeof(T)}] (InQ) are retrieved from queue: {destination.Dump()}");
				while (!this.cancellationTokenSource.IsCancellationRequested && response == null) {
					Task.Delay(500);
				}
				consumer.Listener -= listener;
			}
			return response;
		}

		public void Notify(string queueName, ServiceStack.Messaging.IMessage message)
		{
			throw new NotImplementedException();
		}

		public override void Dispose()
		{
			base.Dispose();
		}

		
	}
}