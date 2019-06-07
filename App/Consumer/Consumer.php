<?php

namespace App\Consumer;

use App\Events\BaseRecord;
use App\SerializerFactory;
use FlixTech\AvroSerializer\Objects\RecordSerializer;
use RdKafka\Exception as KafkaException;
use RdKafka\KafkaConsumer;

class Consumer
{

    private $config;

    private $serializer;

    private $kafkaConsumer;

    public function __construct(ConsumerConfig $config, RecordSerializer $serializer = null)
    {
        $this->config = $config;
        $this->serializer = $serializer ?? (new SerializerFactory($config))->instance();

    }

    /**
     * @param  string[]  $topics
     * @param  BaseRecord  $record
     * @param  callable  $callback
     *
     * @throws \FlixTech\SchemaRegistryApi\Exception\SchemaRegistryException
     * @throws \RdKafka\Exception
     */
    public function consume(array $topics, BaseRecord $record, callable $callback): void
    {

        $this->kafkaConsumer = new KafkaConsumer($this->config);
        $this->kafkaConsumer->subscribe($topics);

        while (true) {
            $message = $this->kafkaConsumer->consume(100000);
            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    print "\nPARTITION: " . $message->partition;
                    print "\nCONSUMING\n\n";
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
}