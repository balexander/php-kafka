<?php

namespace App\Producer;

use FlixTech\AvroSerializer\Objects\RecordSerializer;
use FlixTech\SchemaRegistryApi\Registry\Cache\AvroObjectCacheAdapter;
use FlixTech\SchemaRegistryApi\Registry\CachedRegistry;
use FlixTech\SchemaRegistryApi\Registry\PromisingRegistry;
use GuzzleHttp\Client;

class ProducerFactory
{

    private $producerConfig;

    public function __construct(ProducerConfig $producerConfig)
    {
        $this->producerConfig = $producerConfig;
    }

    public function instance(): Producer
    {
        return new Producer($this->producerConfig, $this->createSerializer());
    }

    private function createRegistry(): CachedRegistry
    {
        return new CachedRegistry(
          new PromisingRegistry(
            new Client(['base_uri' => $this->producerConfig->getSchemaRegistryUri()])
          ),
          new AvroObjectCacheAdapter()
        );
    }

    private function createSerializer(): RecordSerializer
    {
        return new RecordSerializer(
          $this->createRegistry(),
          [
            RecordSerializer::OPTION_REGISTER_MISSING_SCHEMAS => $this->producerConfig->shouldRegisterMissingSchemas(),
            RecordSerializer::OPTION_REGISTER_MISSING_SUBJECTS => $this->producerConfig->shouldRegisterMissingSubjects()
          ]
        );
    }
}