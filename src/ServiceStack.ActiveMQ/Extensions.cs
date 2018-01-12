using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Apache.NMS;
using ServiceStack.Logging;
using ServiceStack.Text;

namespace ServiceStack.ActiveMq
{
    public static class ActiveMqExtensions
    {

		private static readonly ILog Log = LogManager.GetLogger(typeof(MessageFactory));

		private static Server GetActiveMqServer()
		{
			if (HostContext.AppHost == null)
				return null;

			return HostContext.TryResolve<Messaging.IMessageService>() as Server;
		}

		public static async Task<IConnection> GetConnectionAsync(this MessageFactory messageFactory)
		{
			IConnection connection = null;
			Exception ex = null;
			bool retry = false;
			try
			{
				Log.Info($"Etablish connection to ActiveMQBroker {messageFactory.ConnectionFactory.BrokerUri}");
				connection = messageFactory.ConnectionFactory.CreateConnection(messageFactory.UserName, messageFactory.Password);
				connection.ClientId = messageFactory.GenerateConnectionId();

				return connection;
			}
			catch (NMSConnectionException exc)
			{
				ex = exc;
				retry = true;
			}
			catch (InvalidClientIDException exc)
			{
				ex = exc;
			}
			catch (Exception exc)
			{
				if (exc.Source != "Apache.NMS.Stomp") // Somme methods called by Apache.NMS to Stomp are not implemented
				{
					ex = exc;
					throw;
				}
				else
				{
					Log.Warn(exc.Message);
				}
			}

			if (retry)
			{
				Log.Warn($"[Worker {connection.ClientId}] > {ex.Message} - Retry in 5 seconds");
				new System.Threading.AutoResetEvent(false).WaitOne(NMSConstants.defaultRequestTimeout);
				return await GetConnectionAsync(messageFactory);            // Retry
			}
			else
			{
				Log.Error($"Could not connect to ActiveMQ [{messageFactory.ConnectionFactory.BrokerUri}]", ex.GetBaseException());
			}
			return null;
		}

		internal static async Task OpenAsync(this Producer connector)
		{
			string stage = "Entering method OpenAsync";

			ConnectionInterruptedListener OnInterruption = new ConnectionInterruptedListener(() =>
			{
				connector.State = System.Data.ConnectionState.Broken;
				connector.OnTransportError(new Apache.NMS.NMSConnectionException($"Connection to broker has been interrupted"));
			});
			ConnectionResumedListener OnResume = new ConnectionResumedListener(() =>
			{
				connector.State = System.Data.ConnectionState.Open;
			});
			///Occurs when a message could not be understood
			ExceptionListener OnFetchError = new ExceptionListener((ex) =>
			{
				connector.OnMessagingError(ex);
			});

			connector.State = System.Data.ConnectionState.Connecting;
			try
			{
				Log.Info($"Connecting ActiveMQ Broker... [{connector.Connection.ClientId}]");

				connector.Connection.Start();
				connector.Connection.ConnectionInterruptedListener += OnInterruption;
				connector.Connection.ConnectionResumedListener += OnResume;
				connector.Connection.ExceptionListener += OnFetchError;
				connector.State = System.Data.ConnectionState.Open;

				stage = $"starting session to broker [{connector.Connection.Dump()}]";
				using (connector.Session = connector.Connection.CreateSession())
				{
					if(connector.IsReceiver) // QueueClient
					{
						connector.State = System.Data.ConnectionState.Fetching;
					}
					else // Producer
					{
						connector.State = System.Data.ConnectionState.Executing;
					}

					while (!connector.cancellationTokenSource.IsCancellationRequested) // Checks every half second wether Session must be closed
					{
						await Task.Delay(500, connector.cancellationTokenSource.Token);
					}
				}
			}
			catch (TaskCanceledException)
			{
				// Just leave
			}
			catch (Exception ex)
			{
				Log.Error($"An exception has occured while {stage} for {connector.Connection.Dump()} : {ex.Dump()});");
			}
			finally
			{
				connector.Connection.ConnectionInterruptedListener -= OnInterruption;
				connector.Connection.ConnectionResumedListener -= OnResume;
				connector.Connection.ExceptionListener -= OnFetchError;

			}
		}

		public static async Task CloseAsync(this IConnection connection)
		{

			await Task.Factory.StartNew(() =>
			{
				Log.Debug($"Connection to ActiveMQ (Queue : {""} is shutting down");

				//Close all MQClient queues !!!! Not implemented

				if (connection != null)
				{
					if (connection.IsStarted)
					{
						connection.Stop();
						connection.Close();
					}
					connection.Dispose();
				}
			});
		}

		public static void RegisterDirectExchange(this ISession session, string exchangeName = null)
		{
			//session.ExchangeDeclare(exchangeName ?? QueueNames.Exchange, "direct", durable: true, autoDelete: false, arguments: null);
		}

		public static void RegisterDlqExchange(this ISession session, string exchangeName = null)
		{
			//session.ExchangeDeclare(exchangeName ?? QueueNames.ExchangeDlq, "direct", durable: true, autoDelete: false, arguments: null);
		}

		public static void RegisterTopicExchange(this ISession session, string exchangeName = null)
		{
			//session.ExchangeDeclare(exchangeName ?? QueueNames.ExchangeTopic, "topic", durable: false, autoDelete: false, arguments: null);
		}

		public static void RegisterFanoutExchange(this ISession session, string exchangeName)
		{
			//session.ExchangeDeclare(exchangeName, "fanout", durable: false, autoDelete: false, arguments: null);
		}

		public static void RegisterQueues<T>(this ISession session)
		{
			session.RegisterQueue(Messaging.QueueNames<T>.In);
			session.RegisterQueue(Messaging.QueueNames<T>.Priority);
			session.RegisterTopic(Messaging.QueueNames<T>.Out);
			session.RegisterDlq(Messaging.QueueNames<T>.Dlq);
		}

		public static void RegisterQueues(this ISession session, Messaging.QueueNames queueNames)
		{
			session.RegisterQueue(queueNames.In);
			session.RegisterQueue(queueNames.Priority);
			session.RegisterTopic(queueNames.Out);
			session.RegisterDlq(queueNames.Dlq);
		}

		public static void RegisterQueue(this ISession session, string queueName)
		{

			GetActiveMqServer()?.CreateQueueFilter?.Invoke(queueName,null);

			if (!Messaging.QueueNames.IsTempQueue(queueName)) //Already declared in GetTempQueueName()
			{
				//session.QueueDeclare(queueName, durable: true, exclusive: false, autoDelete: false, arguments: args);
			    ITemporaryQueue tempqueue =	session.CreateTemporaryQueue();
			}

			//session.QueueBind(queueName, Messaging.QueueNames.Exchange, routingKey: queueName);
		}

		public static void RegisterDlq(this ISession session, string queueName)
		{
			var args = new Dictionary<string, object>();

			GetActiveMqServer()?.CreateQueueFilter?.Invoke(queueName, args);

			//session.QueueDeclare(queueName, durable: true, exclusive: false, autoDelete: false, arguments: args);
			//session.QueueBind(queueName, Messaging.QueueNames.ExchangeDlq, routingKey: queueName);
		}

		public static void RegisterTopic(this ISession session, string queueName)
		{
			var args = new Dictionary<string, object>();

			GetActiveMqServer()?.CreateTopicFilter?.Invoke(queueName, args);

			//session.QueueDeclare(queueName, durable: false, exclusive: false, autoDelete: false, arguments: args);
			//session.QueueBind(queueName, Messaging.QueueNames.ExchangeTopic, routingKey: queueName);
		}

		public static void DeleteQueue<T>(this ISession model)
		{
			model.DeleteQueues(Messaging.QueueNames<T>.AllQueueNames);
		}

		public static void DeleteQueues(this ISession session, params string[] queues)
		{

				try
				{
					session.DeleteQueues(queues);

				}
				catch (InvalidDestinationException ex)
				{
					if (!ex.Message.Contains("code=404"))
						throw;
				}
			
		}

		public static void PurgeQueue<T>(this ISession model)
		{
			model.PurgeQueues(Messaging.QueueNames<T>.AllQueueNames);
		}

		public static void PurgeQueues(this ISession session, params string[] queues)
		{
				try
				{
					session.PurgeQueues(queues);
				}
				catch (InvalidDestinationException ex)
				{
					if (!ex.Is404())throw;
				}
		}

		public static void RegisterExchangeByName(this ISession session, string exchange)
		{
			if (exchange.EndsWith(".dlq"))
				session.RegisterDlqExchange(exchange);
			else if (exchange.EndsWith(".topic"))
				session.RegisterTopicExchange(exchange);
			else
				session.RegisterDirectExchange(exchange);
		}

		public static void RegisterQueueByName(this ISession session, string queueName)
		{
			if (queueName.EndsWith(".dlq"))
				session.RegisterDlq(queueName);
			else if (queueName.EndsWith(".outq"))
				session.RegisterTopic(queueName);
			else
				session.RegisterQueue(queueName);
		}

		internal static bool Is404(this InvalidDestinationException  ex)
		{
			return ex.Message.Contains("code=404");
		}

		public static bool IsServerNamedQueue(this string queueName)
		{
			if (string.IsNullOrEmpty(queueName))
			{
				throw new ArgumentNullException("queueName");
			}

			var lowerCaseQueue = queueName.ToLower();
			return lowerCaseQueue.StartsWith("amq.")
				|| lowerCaseQueue.StartsWith(Messaging.QueueNames.TempMqPrefix);
		}

		public static void PopulateFromMessage(this IPrimitiveMap props, IMessage message)
		{
			//props.MessageId = message.Id.ToString();
			//props.Timestamp = new AmqpTimestamp(message.CreatedDate.ToUnixTime());
			//props.Priority = (byte)message.Priority;
			//props.ContentType = MimeTypes.Json;

			//if (message.Body != null)
			//{
			//	props.Type = message.Body.GetType().Name;
			//}

			//if (message.ReplyTo != null)
			//{
			//	props.ReplyTo = message.ReplyTo;
			//}

			//if (message.ReplyId != null)
			//{
			//	props.CorrelationId = message.ReplyId.Value.ToString();
			//}

			//if (message.Error != null)
			//{
			//	if (props.Headers == null)
			//		props.Headers = new Dictionary<string, object>();
			//	props.Headers["Error"] = message.Error.ToJson();
			//}
		}

		public static Messaging.IMessage<T> ToMessage<T>(this Apache.NMS.IObjectMessage msgResult)
		{
			if (msgResult == null)
				return null;
			Guid outValue = Guid.Empty;
			var message = new Messaging.Message<T>((T)msgResult.Body);

			message.Meta = new Dictionary<string, string>();
			message.Meta[QueueClient.MetaOriginMessage]= msgResult.NMSDestination.ToString();

			if (Guid.TryParse(msgResult.NMSMessageId, out outValue)) message.Id = outValue;
			message.CreatedDate = msgResult.NMSTimestamp;
			message.Priority = (long)msgResult.NMSPriority;
			message.ReplyTo = msgResult.NMSReplyTo==null?"": msgResult.NMSReplyTo.ToString();
			message.Tag = msgResult.NMSDeliveryMode.ToString();
			message.RetryAttempts = msgResult.NMSRedelivered ? 1 : 0;
			if(msgResult is Apache.NMS.ActiveMQ.Commands.ActiveMQMessage)
			{
				message.RetryAttempts = ((Apache.NMS.ActiveMQ.Commands.ActiveMQMessage)msgResult).NMSXDeliveryCount;
			}

			if(Guid.TryParse(msgResult.NMSCorrelationID, out outValue)) message.ReplyId = outValue;
			var keyEnumerator = msgResult.Properties.Keys.GetEnumerator();
			while(keyEnumerator.MoveNext())
			{
				message.Meta[keyEnumerator.Current.ToString()] = msgResult.Properties[keyEnumerator.Current.ToString()] == null ? null :
					Text.JsonSerializer.SerializeToString(msgResult.Properties[keyEnumerator.Current.ToString()]);
			}
			//if (props.Headers != null)
			//{
			//	foreach (var entry in props.Headers)
			//	{
			//		if (entry.Key == "Error")
			//		{
			//			var errors = entry.Value;
			//			if (errors != null)
			//			{
			//				var errorBytes = errors as byte[];
			//				var errorsJson = errorBytes != null
			//					? errorBytes.FromUtf8Bytes()
			//					: errors.ToString();
			//				message.Error = errorsJson.FromJson<ResponseStatus>();
			//			}
			//		}
			//		else
			//		{
			//			if (message.Meta == null)
			//				message.Meta = new Dictionary<string, string>();

			//			var bytes = entry.Value as byte[];
			//			var value = bytes != null
			//				? bytes.FromUtf8Bytes()
			//				: entry.Value?.ToString();

			//			message.Meta[entry.Key] = value;
			//		}
			//	}
			//}

			return message;
		}

		public static IObjectMessage CreateMessage (this IMessageProducer producer, ServiceStack.Messaging.IMessage message)
		{
			IObjectMessage msg = producer.CreateObjectMessage(message.Body);
			string guid = !message.ReplyId.HasValue ? null : message.ReplyId.Value.ToString();
			msg.NMSMessageId = message.Id.ToString();
			msg.NMSCorrelationID = guid;

			if (message.Meta != null)
			{
				foreach (var entry in message.Meta)
				{
					var converter = System.ComponentModel.TypeDescriptor.GetConverter(entry.Value.GetType());
					var result = converter.ConvertFrom(entry.Value);
					msg.Properties.SetString(entry.Key, entry.Value);
				}
			}
			return msg;
		}

		public static IEnumerable<Messaging.IMessageQueueClient> Bind<T>(IEnumerable<T> complexObjects)
		{
			return complexObjects.Select(obj => obj.Bind<T>());
		}

		public static Messaging.IMessageQueueClient Bind<T>(this T complexObjects)
		{
			return null;
		}
	}
}