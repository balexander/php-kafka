<?php

namespace App;


use App\Consumer\Consumer;
use App\Consumer\ConsumerConfig;
use App\Events\Poc\User\V1\UserEvent;
use Throwable;

require_once __DIR__ . '/../vendor/autoload.php';


function consume()
{
    $config = new ConsumerConfig('schema-registry:8081', 'broker');
    $config->setGroupId('bba123');
    $topics = ['user-mult'];
    echo 'Consuming topics: ' . implode(',', $topics) . PHP_EOL;
    try {
        $consumer = new Consumer($config, ['user-mult']);
        $consumer->onSuccess(function (UserEvent $userEvent)
        {
            return json_encode($userEvent, JSON_PRETTY_PRINT) . PHP_EOL;
        });
    } catch (Throwable $t) {
        var_dump($t);
    }

    $x = $consumer->consumeUntilEnd(new UserEvent());
    var_dump($x);
}

consume();