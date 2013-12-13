{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Pipes.AMQP where
import Control.Concurrent.Async as Async
import Control.Concurrent.STM as STM
import Control.Exception
import Control.Monad (void)
import Control.Monad.Trans
import Data.Text (Text)
import Network.AMQP hiding (cancelConsumer)
import qualified Network.AMQP as AMQP
import Pipes
import qualified Pipes.Concurrent as C
import qualified Pipes.Prelude as P
import qualified Pipes.Safe as S

-- | Registers a worker thread that an AMQP server will push messages to.
receiveMessages :: MonadIO m => Channel -> Ack -> C.Buffer (Message, Envelope) -> Text -> m (MessageReceiver, Producer (Message, Envelope) m ())
receiveMessages chan ack buff queue = do
  (o, i, seal) <- liftIO (C.spawn' buff)
  let consumer = consumeMsgs chan queue ack (void . STM.atomically . C.send o)
  worker <- liftIO consumer
  return (MessageReceiver worker seal chan, C.fromInput i)

-- | Message receiver handle used to shut down the worker and notify the AMQP server.
data MessageReceiver = MessageReceiver
  { consumerTag :: ConsumerTag
  , consumerSeal :: STM ()
  , consumerChannel :: Channel
  }

-- | End a message reception subscription
terminateReceiver :: MonadIO m => MessageReceiver -> m ()
terminateReceiver c = liftIO $ do
  AMQP.cancelConsumer (consumerChannel c) (consumerTag c)
  atomically $ consumerSeal c

-- | Publish messages to an AMQP server (exchange, routing key, message)
sendMessages :: MonadIO m => Channel -> Consumer (Text, Text, Message) m ()
sendMessages chan = go
  where
    go = do
      (exchange, routingKey, msg) <- await
      liftIO $ publishMsg chan exchange routingKey msg
      go
{-
test = do
  conn <- openConnection "192.168.33.10" "/" "guest" "guest"
  chan <- openChannel conn
  (name, _, _) <- declareQueue chan newQueue
  bindQueue chan name "test_exchange" "*"
  (receiver, worker) <- receiveMessages chan NoAck C.Single name
  tid <- liftIO $ Async.async $ do
    runEffect (worker >-> P.map fst >-> P.print)
    putStrLn "done"
  return (conn, chan, receiver, tid)
-}
