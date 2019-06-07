<?php

namespace App;


use App\Consumer\Consumer;
use App\Consumer\ConsumerConfig;
use App\Events\Poc\Common\SharedMeta;
use App\Events\Poc\User\V1\UserEvent;
use App\Producer\Producer;
use App\Producer\ProducerConfig;
use Faker\Factory;

require_once __DIR__ . '/../vendor/autoload.php';



function consume()
{
    $config = new ConsumerConfig('schema-registry:8081', 'broker');
    $config->set('group.id', 'myConsumerGroup');

    // Initial list of Kafka brokers
    $config->set('metadata.broker.list', 'broker');
//    $topic = 'user-event';
    $topic = 'user-mult';
    echo "Consuming topic: $topic" . PHP_EOL;
    $consumer = new Consumer($config);

    $consumer->consume([$topic], new UserEvent(), function (UserEvent $userEvent)
    {
        echo "Record: " . $userEvent->name() . PHP_EOL;
        echo json_encode($userEvent, JSON_PRETTY_PRINT) . PHP_EOL;
    });
}
consume();