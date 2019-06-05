<?php

namespace App\Producer;

use RdKafka\Conf;

class Config extends Conf
{
    private $schemaRegistryUri;
    private $brokers;
    private $registerMissingSchemas = false;
    private $registerMissingSubjects = false;
    private $kafkaConfig;
    private $logLevel;
    private $partition;

    public function __construct(string $brokers = null, string $schemaRegistryUri = null, Conf $kafkaConfig = null, int $logLevel = LOG_DEBUG)
    {
        //Ignore IDE squiggly, there is a constructor its just not int he stub extension
        parent::__construct();
        $this->brokers = $brokers;
        $this->kafkaConfig = $kafkaConfig;
        $this->schemaRegistryUri = $schemaRegistryUri;
        $this->logLevel = $logLevel;
    }

    public function getSchemaRegistryUri(): string
    {
        return $this->schemaRegistryUri;
    }

    public function shouldRegisterMissingSchemas(bool $registerMissingSchemas = null): bool
    {
        return $this->registerMissingSchemas = $registerMissingSchemas ?? $this->registerMissingSchemas;
    }

    public function shouldRegisterMissingSubjects(bool $registerMissingSubjects = null): bool
    {
        return $this->registerMissingSubjects = $registerMissingSubjects ?? $this->registerMissingSubjects;
    }

    public function setDefaultBrokers($defaultBrokers): void
    {
        $this->defaultBrokers = $defaultBrokers;
    }

    public function getKafkaConfig(): Conf
    {
        return $this->kafkaConfig;
    }

    public function getBrokers(): string
    {
        return $this->brokers;
    }

    public function getLogLevel(): int
    {
        return $this->logLevel;
    }

    public function getPartition(): int
    {
        return $this->partition;
    }

    public function setPartition(int $partition)
    {
        $this->partition = $partition;
        return $this;
    }


}

