<?php

namespace App\Producer;

use RdKafka\Conf as KafkaProducerConfig;

class ProducerConfig extends KafkaProducerConfig
{

    protected const DEFAULT_PARTITION = RD_KAFKA_PARTITION_UA;

    private $schemaRegistryUri;

    private $brokers;

    private $kafkaProducerConfig;

    private $registerMissingSchemas = false;

    private $registerMissingSubjects = false;

    private $logLevel;

    private $partition;

    public function __construct(
      string $schemaRegistryUri,
      string $brokers,
      KafkaProducerConfig $kafkaProducerConfig = null
    ) {
        //Ignore IDE squiggly, there is a constructor its just not int he stub extension
        parent::__construct();
        $this->schemaRegistryUri = $schemaRegistryUri;
        $this->brokers = $brokers;
        $this->kafkaProducerConfig = $kafkaProducerConfig;

    }

    public function getSchemaRegistryUri(): string
    {
        return $this->schemaRegistryUri;
    }

    public function getBrokers(): string
    {
        return $this->brokers;
    }

    public function getKafkaProducerConfig(): KafkaProducerConfig
    {
        return $this->kafkaProducerConfig;
    }

    public function shouldRegisterMissingSchemas(bool $registerMissingSchemas = null): bool
    {
        return $this->registerMissingSchemas = $registerMissingSchemas ?? $this->registerMissingSchemas;
    }

    public function shouldRegisterMissingSubjects(bool $registerMissingSubjects = null): bool
    {
        return $this->registerMissingSubjects = $registerMissingSubjects ?? $this->registerMissingSubjects;
    }

    public function getLogLevel(): ?int
    {
        return $this->logLevel;
    }

    public function setLogLevel(int $logLevel): ProducerConfig
    {
        $this->logLevel = $logLevel;
        return $this;
    }

    public function getPartition(): int
    {
        return $this->partition ?? static::DEFAULT_PARTITION;
    }

    public function setPartition(int $partition)
    {
        $this->partition = $partition;
        return $this;
    }
}

