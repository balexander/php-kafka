<?php

namespace App;

use RdKafka\Conf as KafkaConfig;

class Config extends KafkaConfig
{

    protected $schemaRegistryUri;

    protected $brokers;

    protected $kafkaConfig;

    protected $registerMissingSchemas = false;

    protected $registerMissingSubjects = false;

    // todo -- option to set default brokers using metadata.broker.list
    public function __construct(string $schemaRegistryUri, string $brokers)
    {
        //Ignore IDE squiggly, there is a constructor its just not int he stub extension
        parent::__construct();
        $this->schemaRegistryUri = $schemaRegistryUri;
        $this->brokers = $brokers;
    }

    public function getSchemaRegistryUri(): string
    {
        return $this->schemaRegistryUri;
    }

    public function getBrokers(): string
    {
        return $this->brokers;
    }

    public function shouldRegisterMissingSchemas(bool $registerMissingSchemas = null): bool
    {
        return $this->registerMissingSchemas = $registerMissingSchemas ?? $this->registerMissingSchemas;
    }

    public function shouldRegisterMissingSubjects(bool $registerMissingSubjects = null): bool
    {
        return $this->registerMissingSubjects = $registerMissingSubjects ?? $this->registerMissingSubjects;
    }
}