<?php

namespace App;


use App\Consumer\Consumer;
use App\Consumer\ConsumerConfig;
use App\Events\Poc\User\V1\UserEvent;

require_once __DIR__ . '/../vendor/autoload.php';


function consume()
{
    $config = new ConsumerConfig('schema-registry:8081', 'broker');
    $config->set('group.id', 'myConsumerGroup');

    $topics = ['test123'];
    echo 'Consuming topics: ' . implode(',', $topics) . PHP_EOL;
    $consumer = new Consumer($config);

    $consumer->consume($topics, new UserEvent(), static function (UserEvent $userEvent)
    {
        echo 'Record: ' . $userEvent->name() . PHP_EOL;
        echo json_encode($userEvent, JSON_PRETTY_PRINT) . PHP_EOL;
    });
}

consume();