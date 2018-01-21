using Apache.NMS;
using ServiceStack.Text;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ServiceStack.ActiveMq
{
	internal partial class Producer : IDisposable
	{
		internal CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();

		public event EventHandler<System.Data.ConnectionState> ConnectionStateChanged;

		private System.Data.ConnectionState _state = System.Data.ConnectionState.Closed;
		public System.Data.ConnectionState State
		{
			get
			{
				return _state;
			}
			internal set
			{
				bool raise = _state != value;
				System.Data.ConnectionState oldstate = _state;
				_state = value;
				if (raise)
				{
					Log.Debug($"Active MQ Connector [{this.ConnectionName}] has changed from [{oldstate.ToString()}] to [{_state.ToString()}]");
					ConnectionStateChanged?.Invoke(this, value);
				}
			}
		}

		protected Apache.NMS.DestinationType QueueType = Apache.NMS.DestinationType.Queue;

		public string ConnectionName { get; private set; }

		public Apache.NMS.ISession Session { get; internal set; }

		private IConnection connection;
		public IConnection Connection
		{
			get
			{
				if (connection == null)
				{
					connection = msgFactory.GetConnectionAsync().Result;
				}
				return connection;
			}
			internal set
			{
				connection = value;
			}
		}

		internal void ConnectAsync()
		{
			if (this.Connection.IsStarted) return;

			string stage = "Entering method OpenAsync";
			this.State = System.Data.ConnectionState.Connecting;
			try
			{
				Log.Info($"Connecting ActiveMQ Broker... [{this.Connection.ClientId}]");

				this.Connection.Start();
				this.ConnectionName = this.Connection.ClientId;
				this.Connection.ConnectionInterruptedListener += Connection_ConnectionInterruptedListener;
				this.Connection.ConnectionResumedListener += Connection_ConnectionResumedListener;
				this.Connection.ExceptionListener += Connection_ExceptionListener;

				this.State = System.Data.ConnectionState.Open;

				stage = $"starting session to broker [{this.Connection.Dump()}]";

			}
			catch (TaskCanceledException)
			{
				Log.Info($"Safe connection close for {this.Connection.ClientId} while {stage});");
			}
			catch (Exception ex)
			{
				Log.Error($"An exception has occured while {stage} for {this.Connection.Dump()} : {ex.Dump()});");
			}
		}

		internal void CloseConnection()
		{
			//Log.Debug($"Connection to ActiveMQ (Queue : {Connection.ClientId} is shutting down");

			this.Connection.ConnectionInterruptedListener -= Connection_ConnectionInterruptedListener;
			this.Connection.ConnectionResumedListener -= Connection_ConnectionResumedListener;
			this.Connection.ExceptionListener -= Connection_ExceptionListener;
			//Close all MQClient queues !!!! Not implemented

			if (Connection != null)
			{
				if (Connection.IsStarted)
				{
					Connection.Stop();
					Connection.Close();
				}
				Connection.Dispose();
			}

		}

		private void Connection_ExceptionListener(Exception exception)
		{
			this.OnMessagingError(exception);
		}

		private void Connection_ConnectionResumedListener()
		{
			this.State = System.Data.ConnectionState.Open;
		}

		private void Connection_ConnectionInterruptedListener()
		{
			this.State = System.Data.ConnectionState.Broken;
			this.OnTransportError(new Apache.NMS.NMSConnectionException($"Connection to broker has been interrupted"));
		}

		internal async Task OpenSessionAsync()
		{
			this.ConnectAsync();
			if (this.Session != null)
			{
				dynamic session = this.Session;
				if (session.Started) return;
			}

			using (this.Session = this.Connection.CreateSession())
			{

				if (this.IsConsumer) // QueueClient
				{
					this.State = System.Data.ConnectionState.Fetching;
				}
				else // Producer
				{
					this.State = System.Data.ConnectionState.Executing;
				}

				while (!this.cancellationTokenSource.IsCancellationRequested) // Checks every half second wether Session must be closed
				{
					await Task.Delay(500, this.cancellationTokenSource.Token);
				}
			}
		}


		#region Consumer
		/// <summary>
		// Turn received Message into expected type of the ServiceStackMessageHandler<T>
		/// </summary>
		/// <param name="apsession"></param>
		/// <param name="producer"></param>
		/// <param name="message"></param>
		/// <returns>An Apache.NMS.IObjectMessage</returns>
		internal Apache.NMS.ConsumerTransformerDelegate CreateConsumerTransformer()
		{
			return new Apache.NMS.ConsumerTransformerDelegate((apsession, consumer, message) =>
			{
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
			});
		}

		protected static SemaphoreSlim semaphoreConsumer = null;
		Apache.NMS.IMessageConsumer _consumer = null;
		internal Apache.NMS.IMessageConsumer Consumer
		{
			get
			{
				if (_consumer == null)
				{
					string queuename = this.ResolveQueueNameFn(msgHandler.MessageType.Name, ".inq");
					Apache.NMS.IDestination destination = null;
					try
					{
						semaphoreConsumer.Wait();
						destination = Apache.NMS.Util.SessionUtil.GetDestination(this.Session, queuename);
						_consumer = this.Session.CreateConsumer(destination);
						_consumer.ConsumerTransformer = CreateConsumerTransformer();
						Log.Debug($"A Consumer {_consumer.Dump()} has been created to listen on queue [{queuename}].");
					}
					catch (Exception ex)
					{
						Log.Warn($"A problem occured while creating a Consumer on queue [{queuename}]: {ex.GetBaseException().Message}");
						return _consumer;
					}
					finally
					{
						semaphoreConsumer.Release();
					}
				}
				return _consumer;
			}
		}

		#endregion

		#region Producer


		internal Apache.NMS.ProducerTransformerDelegate CreateProducerTransformer()
		{
			return new ProducerTransformerDelegate((session, producer, message) =>
			{
				IObjectMessage obj = message as IObjectMessage;
				obj.Body = JsonSerializer.SerializeToString(obj.Body);
				return obj;
			});
		}

		static SemaphoreSlim semaphoreProducer = null;

		internal async Task<Apache.NMS.IMessageProducer> GetProducer(string queuename)
		{
			Apache.NMS.IMessageProducer _producer = null;
			IDestination destination = null;
			try
			{
				await OpenSessionAsync();
				semaphoreProducer.Wait();
				destination = this.Session.GetDestination(queuename);
				_producer = this.Session.CreateProducer(destination);
				_producer.ProducerTransformer = CreateProducerTransformer();
				return _producer;
				//}
			}
			catch (Exception ex)
			{
				Log.Warn($"A problem occured while creating a Producer on queue {queuename}: {ex.GetBaseException().Message}");
				return await GetProducer(queuename);
			}
			finally
			{
				semaphoreProducer.Release();
			}
		}
		#endregion

		#region IDisposable Support
		private bool disposedValue = false; // To detect redundant calls

		protected virtual void Dispose(bool disposing)
		{
			if (!disposedValue)
			{
				if (disposing)
				{
					
					// Close Listening Thread
					cancellationTokenSource.Cancel();
					//this.Connection.Stop();
					//this.Connection.Dispose();
					if (_consumer != null) _consumer.Dispose();
					Log.Info($"Close connection : [{this.ConnectionName}]");
					this.Connection = null;
					this.State = System.Data.ConnectionState.Closed;
				}
				// TODO: free unmanaged resources (unmanaged objects) and override a finalizer below.
				// TODO: set large fields to null.
				disposedValue = true;
			}
		}

		// TODO: override a finalizer only if Dispose(bool disposing) above has code to free unmanaged resources.
		// ~Producer() {
		//   // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
		//   Dispose(false);
		// }

		// This code added to correctly implement the disposable pattern.
		public void Dispose()
		{
			// Do not change this code. Put cleanup code in Dispose(bool disposing) above.
			Dispose(true);
			// TODO: uncomment the following line if the finalizer is overridden above.
			GC.SuppressFinalize(this);
			GC.Collect();
		}
		#endregion

	}
}
