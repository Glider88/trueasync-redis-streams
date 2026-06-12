<?php declare(strict_types=1);

namespace Glider88\AsyncRedisStreams;

use Glider88\AsyncRedisStreams\Helpers\Time;
use Glider88\AsyncRedisStreams\Redis\Redis;
use function Async\{spawn_thread, delay};

readonly class AutoClaim
{
    public const string FIRST_ID = '0-0';

    public function __construct(
        private Config $config,
    ) {}

    public function run(): void
    {
        $cfg = $this->config;
        spawn_thread(
            task: static function() use ($cfg) {
                $redis = Redis::fromConfig($cfg);
                while (true) {
                    delay($cfg->claimInterval->milli());
                    $start = AutoClaim::FIRST_ID;
                    while (true) {
                        if ($redis->isConsumerCanceled($cfg->consumer)) {
                            return;
                        }

                        $cfg->logger->debug('new auto claim loop tick');
                        $res = $redis->autoClaim($start);

                        $start = $res[0] ?? AutoClaim::FIRST_ID;
                        $entries = $res[1] ?? [];
                        $cfg->logger->debug('count claim: ' . count($entries));

                        $now = Time::nowMs();
                        foreach ($entries as $id => $fields) {
                            $fields['_service_data_message_retries'] += 1;
                            if ($fields['_service_data_message_retries'] > $cfg->maxRetries) {
                                $fields['_service_data_reason'] = 'max retries reached';
                                $redis->addToDlq($fields);
                                $redis->ack($id);
                                $cfg->logger->error(
                                    'message#' . $fields['_service_data_message_id'] . 'go to dead letter queue'
                                );

                                continue;
                            }

                            $cfg->logger->debug("put from stream to retry: " . $id);
                            $redis->revokeGuard($fields['_service_data_message_id']);
                            $startAt = $now + $cfg->retry->delay($fields['_service_data_message_retries'])->milli();
                            $redis->addToRetry($startAt, $fields);
                            $redis->ack($id);
                        }

                        if ($start === AutoClaim::FIRST_ID) {
                            $cfg->logger->debug('claim meet 0-0');

                            break;
                        }
                    }
                }
            },
            bootloader: $this->config->threadBootloader,
        );
    }
}
