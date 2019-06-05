<?php

namespace App\Producer;

use App\Producer\Config;
use App\Events\BaseRecord;
use App\Events\Poc\Common\SharedMeta;
use App\Events\Poc\User\V1\UserEvent;
//use App\Events\Poc\User\V2\UserEvent;
use AvroSchema;
use Exception;
use Faker\Factory;
use FlixTech\AvroSerializer\Objects\RecordSerializer;
use FlixTech\SchemaRegistryApi\Registry;
use FlixTech\SchemaRegistryApi\Registry\Cache\AvroObjectCacheAdapter;
use FlixTech\SchemaRegistryApi\Registry\CachedRegistry;
use FlixTech\SchemaRegistryApi\Registry\PromisingRegistry;
use GuzzleHttp\Client;
use Noodlehaus\Parser\Yaml;
use RdKafka\Conf;
use RdKafka\Message;
use \RdKafka\Producer as KafkaProducer;


class Producer {


    private $config;
    private $recordSerializer;
    private $brokers;

    public function __construct(Config $config, RecordSerializer $serializer = null, Registry $registry = null) {
        $this->config = $config;
        $this->brokers = $config->getBrokers();

        $this->recordSerializer = $this->getSerializer($this->getRegistry());
        $this->kafkaProducer = $this->createKafkaProducer();

    }

    public function fire(string $topic, BaseRecord $record): void
    {
        $topicProducer = $this->kafkaProducer->newTopic($topic);
        $encodedRecord = $this->encodeRecord($record);
        $topicProducer->produce(RD_KAFKA_PARTITION_UA, 0, $encodedRecord, $record->getKey());

    }

    private function getRegistry(): CachedRegistry
    {
        return new CachedRegistry(
          new PromisingRegistry(
            new Client(['base_uri' => $this->config->getSchemaRegistryUri()])
          ),
          new AvroObjectCacheAdapter()
        );
    }

    private function getSerializer(Registry $registry): RecordSerializer
    {
        return new RecordSerializer(
          $registry,
          [
            RecordSerializer::OPTION_REGISTER_MISSING_SCHEMAS => true,//$this->config->shouldRegisterMissingSchemas(),
            RecordSerializer::OPTION_REGISTER_MISSING_SUBJECTS => true//$this->config->shouldRegisterMissingSchemas(),
          ]
        );
    }

    private function createKafkaProducer(): KafkaProducer
    {
        $producer = new KafkaProducer($this->config);
        if ($this->brokers) {
            $producer->addBrokers($this->brokers);
        }

        return $producer;
    }

    private function encodeRecord(BaseRecord $record): string
    {
        $schema = AvroSchema::parse($record->schema());
        $data = $record->data();
        $name = str_replace('_', '-', snake($record->name()));
        return $this->recordSerializer->encodeRecord($name.'-value', $schema, $data);
    }

}

function snake($value, $delimiter = '_')
{
    if (! ctype_lower($value)) {
        $value = preg_replace('/\s+/u', '', ucwords($value));

        $value = preg_replace('/(.)(?=[A-Z])/u', '$1'.$delimiter, $value);
        return mb_strtolower($value, 'UTF-8');
    }

    return $value;
}
