<?php

namespace App\Producer;

use FlixTech\AvroSerializer\Objects\RecordSerializer;
use FlixTech\SchemaRegistryApi\Registry;
use RdKafka\Conf;
use RdKafka\Message;

class ProducerFactory {

    public function __construct(Conf $producCconfig = null) {
        
    }

    public static function instance(Config $config = null, RecordSerializer $serializer = null, Registry $registry = null) : Producer {
//        if (!$config) {
//            $config = new Conf();
//            $config->setDrMsgCb(function ($kafka, Message $message) {
//                if ($message->err) {
//                    // message permanently failed to be delivered
//                    print 'ok it did work ';
//                } else {
//                    print 'ok it worked ';
//                    return 'bba';
//                }
//            });
//        }
        return new Producer($config, $serializer, $registry);
    }
}