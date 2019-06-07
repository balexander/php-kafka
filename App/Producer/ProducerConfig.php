<?php

namespace App\Producer;

use App\Config;
use RdKafka\Conf as KafkaConfig;

class ProducerConfig extends Config
{

    protected const DEFAULT_PARTITION = RD_KAFKA_PARTITION_UA;

    /**
     *
     * todo -- look into these options more.
     * msgflags - 0, or one of:

    RD_KAFKA_MSG_F_COPY - librdkafka will immediately make a copy of the payload. Use this when the payload is in non-persistent memory, such as the stack.
    RD_KAFKA_MSG_F_FREE - let librdkafka free the payload using free(3) when it is done with it.
    These two flags are mutually exclusive and neither need to be set in which case the payload is neither copied nor freed by librdkafka.

    If RD_KAFKA_MSG_F_COPY flag is not set no data copying will be performed and librdkafka will hold on the payload pointer until the message has been delivered or fails. The delivery report callback will be called when librdkafka is done with the message to let the application regain ownership of the payload memory. The application must not free the payload in the delivery report callback if RD_KAFKA_MSG_F_FREE is set.


     */
    protected const DEFAULT_MESSAGE_FLAG = 0;

    private $logLevel;

    private $partition;

    private $messageFlag;

    public function __construct(
      string $schemaRegistryUri,
      string $brokers
//      KafkaConfig $kafkaConfig = null
    ) {
        //Ignore IDE squiggly, there is a constructor its just not int he stub extension
        parent::__construct($schemaRegistryUri, $brokers);
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

    public function getMessageFlag(): int
    {
        return $this->messageFlag ?? static::DEFAULT_MESSAGE_FLAG;
    }

    public function setMessageFlag($messageFlag)
    {
        // todo -- validation
        $this->messageFlag = $messageFlag;
        return $this;
    }
}

