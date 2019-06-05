<?php

namespace App\Producer;

use App\Events\BaseRecord;
use AvroSchema;
use FlixTech\AvroSerializer\Objects\RecordSerializer;

use \RdKafka\Producer as KafkaProducer;


class Producer
{


    private $config;

    private $serializer;

    private $kafkaProducer;

    public function __construct(ProducerConfig $config, RecordSerializer $serializer = null)
    {
        $this->config = $config;
        $this->serializer = $serializer;
        $this->kafkaProducer = $this->createKafkaProducer();
    }

    public function fire(string $topic, BaseRecord $record): void
    {
        $topicProducer = $this->kafkaProducer->newTopic($topic);
        $encodedRecord = $this->encodeRecord($record);
        $topicProducer->produce(RD_KAFKA_PARTITION_UA, 0, $encodedRecord, $record->getKey());

    }

    private function createKafkaProducer(): KafkaProducer
    {
        $producer = new KafkaProducer($this->config);

        $brokers = $this->config->getBrokers();
        if ($brokers) {
            $producer->addBrokers($brokers);
        }

        $logLevel = $this->config->getLogLevel();
        if ($logLevel) {
            $producer->setLogLevel(LOG_DEBUG);
        }

        return $producer;
    }

    private function encodeRecord(BaseRecord $record): string
    {
        $schema = AvroSchema::parse($record->schema());
        $data = $record->data();
        $name = str_replace('_', '-', $this->convertToSnakeCase($record->name()));

        return $this->serializer->encodeRecord($name . '-value', $schema, $data);
    }

    private function convertToSnakeCase($value, $delimiter = '_')
    {
        if (!ctype_lower($value)) {
            $value = preg_replace('/\s+/u', '', ucwords($value));

            $value = preg_replace('/(.)(?=[A-Z])/u', '$1' . $delimiter, $value);
            return mb_strtolower($value, 'UTF-8');
        }

        return $value;
    }

}