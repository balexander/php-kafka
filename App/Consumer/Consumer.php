<?php

namespace App\Consumer;

use App\Events\BaseRecord;
use App\SerializerFactory;
use FlixTech\AvroSerializer\Objects\RecordSerializer;
use RdKafka\Exception as KafkaException;
use RdKafka\KafkaConsumer;

class Consumer
{

    private $serializer;

    private $kafkaConsumer;

    public function __construct(ConsumerConfig $config, RecordSerializer $serializer = null)
    {
        $this->serializer = $serializer ?? (new SerializerFactory($config))->instance();
        $this->kafkaConsumer = new KafkaConsumer($config);
    }

    /**
     * @param  string[]  $topics
     * @param  BaseRecord  $record
     * @param  callable  $callback
     *
     * @throws \FlixTech\SchemaRegistryApi\Exception\SchemaRegistryException
     * @throws \RdKafka\Exception
     */
    public function consume(array $topics, BaseRecord $record, callable $callback = null): void
    {
        $this->kafkaConsumer->subscribe($topics);

        while (true) {
            $message = $this->kafkaConsumer->consume(100000);
            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    $decoded = $this->serializer->decodeMessage($message->payload);
                    $record->decode($decoded);
                    $callback($record);
                    break;
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    echo "No more messages; will wait for more\n";
                    break;
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    echo "Timed out\n";
                    break;
                default:
                    throw new KafkaException($message->errstr(), $message->err);
                    break;
            }
        }
    }

    private function determineMaxPartitions(array $topics = null)
    {
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