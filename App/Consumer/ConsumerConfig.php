<?php

namespace App\Consumer;

use App\Config;
use RdKafka\TopicConf;

class ConsumerConfig extends Config
{

    //    protected const DEFAULT_PARTITION = 0;

    protected const DEFAULT_OFFSET = RD_KAFKA_OFFSET_BEGINNING;

    // todo - what is a sane default timeout?
    protected const DEFAULT_TIMEOUT = 1000;

    private $partition;

    private $offset;

    private $timeout;

    private $topicConfig;

    private $numConsumers = 1;

    public function __construct(string $schemaRegistryUri, string $brokers, TopicConf $defaultTopicConfig = null)
    {
        //Ignore IDE squiggly, there is a constructor its just not int he stub extension
        parent::__construct($schemaRegistryUri, $brokers);
        $defaultTopicConfig = $defaultTopicConfig ?? $this->createDefaultTopicConfig();
        $this->setDefaultTopicConf($defaultTopicConfig);
    }

    //    public function getPartition(): int
    //    {
    //        return $this->partition ?? static::DEFAULT_PARTITION;
    //    }

    public function setPartition(int $partition)
    {
        $this->partition = $partition;
        return $this;
    }

    public function getOffset(): int
    {
        return $this->offset ?? static::DEFAULT_OFFSET;
    }

    public function setOffset(int $offset)
    {
        $this->offset = $offset;
        return $this;
    }

    public function getTimeout(): int
    {
        return $this->timeout ?? static::DEFAULT_TIMEOUT;
    }

    public function setTimeout(int $timeout)
    {
        $this->timeout = $timeout;
        return $this;
    }

    public function getTopicConfig(): TopicConf
    {
        // todo -- make sane default topic config
        return $this->topicConfig ?? $this->createDefaultTopicConfig();
    }

    public function setTopicConfig($topicConfig)
    {
        $this->topicConfig = $topicConfig;
        return $this;
    }

    // todo -- movethis
    private function createDefaultTopicConfig(): TopicConf
    {
        $topicConfig = new TopicConf();
        $topicConfig->set('auto.offset.reset', 'smallest');
        return $topicConfig;
    }

    public function getNumConsumers()
    {
        return $this->numConsumers;
    }

}