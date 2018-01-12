using ServiceStack.Logging;
using ServiceStack.Messaging;
using System;
using System.Collections.Generic;
using System.Linq;


namespace ServiceStack.ActiveMq
{
	public class Server : IMessageService, IDisposable
	{

		private static readonly ILog Log = LogManager.GetLogger(typeof(Server));


		public const int DefaultRetryCount = 1; //Will be a total of 2 attempts

		/// <summary>
		/// Execute global transformation or custom logic before a request is processed.
		/// Must be thread-safe.
		/// </summary>
		public Func<IMessage, IMessage> RequestFilter { get; set; }

		/// <summary>
		/// Execute global transformation or custom logic on the response.
		/// Must be thread-safe.
		/// </summary>
		public Func<object, object> ResponseFilter { get; set; }

		public Action<string, Apache.NMS.IPrimitiveMap, IMessage> PublishMessageFilter
		{
			get { return messageFactory.PublishMessageFilter; }
			set { messageFactory.PublishMessageFilter = value; }
		}

		public Action<string, ServiceStack.Messaging.IMessage> GetMessageFilter
		{
			get { return messageFactory.GetMessageFilter; }
			set { messageFactory.GetMessageFilter = value; }
		}

		public Func<object, string, string> ResolveQueueNameFn
		{
			get { return messageFactory.ResolveQueueNameFn; }
			set { messageFactory.ResolveQueueNameFn = value; }
		}

		public Action<string, Dictionary<string, object>> CreateQueueFilter { get; set; }
		public Action<string, Dictionary<string, object>> CreateTopicFilter { get; set; }

		public Server(string connectionString = "tcp://localhost:61616", string username = null, string password = null): this(
			new ActiveMq.MessageFactory(new Apache.NMS.NMSConnectionFactory(connectionString)))
		{

		}

		public Server(ActiveMq.MessageFactory messageFactory)
		{
			this.messageFactory = messageFactory;
			this.ResolveQueueNameFn = (type, suffix) => ServiceStack.Messaging.QueueNames.ResolveQueueNameFn(type as string, suffix);
			this.ErrorHandler = (worker, ex) => Log.Error("Exception in Active MQ Plugin: ", ex);
		}

		protected IMessageHandlerFactory CreateMessageHandlerFactory<T>(Func<IMessage<T>, object> processMessageFn, Action<IMessageHandler, IMessage<T>, Exception> processExceptionEx)
		{
			return new MessageHandlerFactory<T>(this, processMessageFn, processExceptionEx)
			{
				RequestFilter = this.RequestFilter,
				ResponseFilter = this.ResponseFilter
				//PublishResponsesWhitelist = PublishResponsesWhitelist,
				//RetryCount = RetryCount,
			};
		}

		/// <summary>
		/// The Message Factory used by this MQ Server
		/// </summary>
		private MessageFactory messageFactory;
		public IMessageFactory MessageFactory => messageFactory;

		/// <summary>
		/// Execute global error handler logic. Must be thread-safe.
		/// </summary>
		internal Action<Worker, Exception> ErrorHandler { get; set; }

		public List<Type> RegisteredTypes => handlerMap.Keys.ToList();

		private readonly Dictionary<Type, Tuple<IMessageHandlerFactory, Worker[]>> handlerMap = new Dictionary<Type, Tuple<IMessageHandlerFactory, Worker[]>>();

		public IMessageHandlerStats GetStats()
		{
			throw new NotImplementedException();
		}

		public string GetStatsDescription()
		{
			throw new NotImplementedException();
		}

		public string GetStatus()
		{
			throw new NotImplementedException();
		}

		public void RegisterHandler<T>(Func<IMessage<T>, object> processMessageFn)
		{
			RegisterHandler(processMessageFn, null, noOfThreads: -1);
		}

		public void RegisterHandler<T>(Func<IMessage<T>, object> processMessageFn, int noOfThreads)
		{
			RegisterHandler(processMessageFn, null, noOfThreads);
		}

		public void RegisterHandler<T>(Func<IMessage<T>, object> processMessageFn, Action<IMessageHandler, IMessage<T>, Exception> processExceptionEx)
		{
			RegisterHandler(processMessageFn, processExceptionEx, noOfThreads: -1);
		}

		public void RegisterHandler<T>(Func<IMessage<T>, object> processMessageFn, Action<IMessageHandler, IMessage<T>, Exception> processExceptionEx, int noOfThreads)
		{
			if (handlerMap.ContainsKey(typeof(T)))
				throw new ArgumentException("Message handler has already been registered for type: " + typeof(T).Name);
			//Checl licence validity before instantiating anything
			LicenseUtils.AssertValidUsage(LicenseFeature.ServiceStack, QuotaType.Operations, handlerMap.Count +1);

			if (noOfThreads <= 0) noOfThreads = Environment.ProcessorCount;
			IMessageHandlerFactory handlerMessageFactory = CreateMessageHandlerFactory<T>(processMessageFn, processExceptionEx);
			handlerMap[typeof(T)] = new Tuple<IMessageHandlerFactory, Worker[]>(handlerMessageFactory, new Worker[noOfThreads]);

		}

		public void Start()
		{
			handlerMap.Select(kv => kv.Value)
				.ToList()
				.ForEach(async tuple => {
					for(int i= 0;i< tuple.Item2.Length; i++)
					{
						tuple.Item2[i] = await Worker.StartAsync(this, tuple.Item1);
					}
			}); ;
		}

		public void Stop()
		{
			handlerMap.Values.SelectMany(item=>item.Item2).ToList().ForEach(
				worker =>
				{
					worker.Dispose();
				}
			);
		}

		public void Dispose()
		{
			this.messageFactory.Dispose();
			this.Dispose();
			GC.Collect();
		}
	}
}
