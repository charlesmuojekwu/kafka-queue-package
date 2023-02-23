<?php

namespace Kafka;

use Illuminate\Queue\Queue;
use Illuminate\Contracts\Queue\Queue as QueueContract;


class KafkaQueue extends Queue implements QueueContract
{

    protected $producer, $consumer;

    public function __construct($producer,$consumer)
    {
        $this->producer = $producer;
        $this->consumer = $consumer;

    }

    public function size($queue = null){}


    public function push($job, $data = '', $queue = null){
        $topic = $this->producer->newTopic($queue ?? env('KAFKA_QUEUE'));

        $n = 0;
        while($n < 2){
        $message = readline("write a message:");
        $topic->produce(RD_KAFKA_PARTITION_UA, 0,  serialize($job));
        $this->producer->flush(1000);

        $n++;
        }
    }


    ///public function pushOn($queue, $job, $data = ''){}


    public function pushRaw($payload, $queue = null, array $options = []){}


    public function later($delay, $job, $data = '', $queue = null){}


    public function pop($queue = null){
        $this->consumer->subscribe([$queue]); // subcribe to producer[default] created in conflunce portal

        try{
            $message = $this->consumer->consume(120 * 1000); //consume in milliseconds

            switch ($message->err){
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    var_dump($message->payload);
                    break;
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    var_dump( "No more message: wiil wait for more\n");
                    break;
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    var_dump(  "Timed out\n" );
                    break;
                default:
                    throw new \Exception($message->errstr(), $message->err);
                    break;
            }
        }catch(\Exception $e){
            var_dump($e->getMessage());
        }
    }

}
