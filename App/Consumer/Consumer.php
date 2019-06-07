<?php

namespace App\Consumer;

use App\Events\BaseRecord;
use App\SerializerFactory;
use FlixTech\AvroSerializer\Objects\RecordSerializer;
use RdKafka\Exception as KafkaException;
use RdKafka\KafkaConsumer;
use RdKafka\Message;

class Consumer
{

    private $serializer;

    private $kafkaConsumer;

    private $config;

    private $errorCallback;

    private $successCallback;

    public function __construct(ConsumerConfig $config, array $topics, RecordSerializer $serializer = null)
    {
        $this->config = $config;
        $this->serializer = $serializer ?? (new SerializerFactory($config))->instance();
        $this->kafkaConsumer = new KafkaConsumer($config);
        $this->kafkaConsumer->subscribe($topics);
    }

    public function consumeSingle(BaseRecord $record)
    {
        $message = $this->kafkaConsumer->consume($this->config->getTimeout());
        return $this->handleMessage($message, $record);
    }

    public function consumeUntilEnd(BaseRecord $record): array
    {
        $results = [];

        $message = $this->kafkaConsumer->consume($this->config->getTimeout());
        print "initial msg $message->err";
        while ($message->err !== RD_KAFKA_RESP_ERR__PARTITION_EOF) {
            $results[] = $this->handleMessage($message, $record);
            $message = $this->kafkaConsumer->consume($this->config->getTimeout());
        }

        return $results;
    }

    public function consumeForever(BaseRecord $record): void
    {
        while (true) {
            $message = $this->kafkaConsumer->consume($this->config->getTimeout());
            $this->handleMessage($message, $record);
        }
    }

    private function handleMessage(Message $message, BaseRecord $record)
    {
        switch ($message->err) {
            case RD_KAFKA_RESP_ERR_NO_ERROR:
                $decoded = $this->serializer->decodeMessage($message->payload);
                $record->decode($decoded);
                return ($this->successCallback)($record);
                break;
            case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                echo "No more messages; will wait for more\n";
                break;
            case RD_KAFKA_RESP_ERR__TIMED_OUT:
                echo "Timed out\n";
                break;
            default:
                if ($this->errorCallback) {
                    ($this->errorCallback)();
                }
                throw new KafkaException($message->errstr(), $message->err);
                break;
        }
    }

    public function onError(callable $callback): void
    {
        $this->errorCallback = $callback;
    }

    public function onSuccess(callable $callback): void
    {
        $this->successCallback = $callback;
    }

    private function getPartitionsInfo()
    {
        $partitionsInfo = [];

        foreach ($this->kafkaConsumer->getMetadata(true, null, 10000)->getTopics() as $topic) {
            $partitionsInfo[$topic->getTopic()] = count($topic->getPartitions());
        }

        return $partitionsInfo;
    }

    private function determineMaxPartitions(
      array $topics = null
    ) {
        $metaData = $this->kafkaConsumer->getMetadata(false, null, 12000);
        $allTopics = $metaData->getTopics();
        $max = 1;
        foreach ($allTopics as $topic) {
            /** @var \RdKafka\Metadata\Topic $topic */
            if (!in_array($topic->getTopic(), $topics)) {
                continue;
            }
            $numPartitions = count($topic->getPartitions());
            if ($numPartitions > $max) {
                $max = $numPartitions;
            }
        }
        return $max;

    }
}