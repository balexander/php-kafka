<?php

namespace App;


use App\Consumer\Consumer;
use App\Consumer\ConsumerConfig;
use App\Events\Poc\Common\SharedMeta;
use App\Events\Poc\User\V1\UserEvent;
use App\Producer\Producer;
use App\Producer\ProducerConfig;
use App\Producer\ProducerFactory;
use DateTime;
use Faker\Factory;

require_once __DIR__ . '/../vendor/autoload.php';


function produce() {
//    $topic = 'user-event';
    $topic = 'user-mult';
    $config = new ProducerConfig('schema-registry:8081', 'broker');
    $config->shouldRegisterMissingSchemas(true);
    $config->shouldRegisterMissingSubjects(true);

    $producer  = new Producer($config);

    $faker = Factory::create();

    for ($i = 1; $i < 50; $i++) {
        $date = new DateTime();
        $d= $date->format('Y-m-d H:i:s');
        echo "Producing topic: $topic" . PHP_EOL;
        $meta1 = (new SharedMeta())->setUuid($d.'-'.$i);
        $userEventV1 = (new UserEvent())->setUserId($faker->randomDigit)->setMeta($meta1);
        $producer->fire($topic, $userEventV1);
        $producer->fire($topic, $meta1);
    }
}

produce();