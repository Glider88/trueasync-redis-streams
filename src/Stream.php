<?php declare(strict_types=1);

namespace Glider88\AsyncRedisStreams;

use Async\Coroutine;
use Async\Scope;
use Glider88\AsyncRedisStreams\Redis\Redis;
use Glider88\AsyncRedisStreams\Redis\RedisInterface;
use Throwable;
use function Async\{delay, suspend};


readonly class Stream
{
    private RedisInterface $redis;

    public function __construct(
        private Config $config,
    ) {
        $this->redis = Redis::fromConfig($this->config);
        $this->redis->createConsumerGroupIfNotExist();
    }

    public static function canceledKey(string $consumer): string
    {
        return "_service_data_is_stream_canceled_$consumer";
    }

    public function push(string $id, array $data): string
    {
        $data['_service_data_message_retries'] = 0;
        $data['_service_data_message_id'] = $id;

        return $this->redis->add($data);
    }

    public function run(MessageHandlerInterface $handler, ?int $times = null): void
    {
        $this->redis->setConsumerCanceled($this->config->consumer, false);
        new Retry($this->config)->run();
        new AutoClaim($this->config)->run();

        $coroutines = [];
        $isStop = false;
        if (extension_loaded('pcntl')) {
            pcntl_async_signals(true);
            pcntl_signal(SIGINT, function () use (&$isStop) {
                $this->config->logger->info("catch SIGINT");
                $isStop = true;
            });
            pcntl_signal(SIGTERM, function () use (&$isStop) {
                $this->config->logger->info("catch SIGTERM");
                $isStop = true;
            });
        }

        while (true) {
            suspend();
            $this->config->logger->debug('new loop tick');
            if ($isStop) {
                $this->config->logger->info("try to stop event loop");
                $this->waitAll($coroutines);
                $this->close();
                break;
            }

            $coroutines = $this->filter($coroutines);

            $lag = $this->lag();
            $concurrency = $this->config->scaling->numberOfWorkers($lag);
            $this->config->logger->debug("lag: $lag, concurrency: $concurrency, coroutines: " . count($coroutines));

            $this->config->logger->debug('read');
            $batch = $this->redis->read($concurrency - count($coroutines));
            if (empty($batch) || !array_key_exists($this->config->stream, $batch)) {
                $this->config->logger->debug('redis stream is empty');
                continue;
            }
            $this->config->logger->debug('messages count: ' . count($batch[$this->config->stream]));

            foreach ($batch[$this->config->stream] as $id => $fields) {
                $messageId = $fields['_service_data_message_id'];
                $ok = $this->redis->passThroughGuard($messageId);
                if (!$ok) {
                    $this->config->logger->warning("duplicate message#$messageId for group: {$this->config->group}, stream: {$this->config->stream}");
                    continue;
                }

                $scope = new Scope();
                $cfg = $this->config;
                $scope->spawn(static function () use ($cfg, $scope) {
                    delay($cfg->timeoutJob->milli());
                    $scope->cancel();
                });
                $redis = $this->redis;
                $c = $scope->spawn(static function () use ($handler, $id, $fields, $scope, $cfg, $redis) {
                    try {
                        unset($fields['_service_data_message_retries'], $fields['_service_data_message_id']);
                        $cfg->logger->debug("start handle message: " . $id);
                        $handler->handle($fields);
                        $cfg->logger->debug("stop handle message: " . $id);
                        $redis->ack($id);
                        $scope->cancel();
                    } catch (Throwable $e) {
                        $cfg->logger->error("FAILED handle message: " . $e->getMessage());
                        $scope->cancel();
                        // not ack -> left in PEL
                    }
                });

                $coroutines[$id] = $c;

                if ($times !== null) {
                    $this->config->logger->debug('times: ' . $times);
                    $times -= 1;
                    if ($times <= 0) {
                        $this->waitAll($coroutines);
                        break 2;
                    }
                }
            }
        }
    }

    /**
     * @param array<string, Coroutine> $coroutines
     * @return array<string, Coroutine>
     */
    private function filter(array $coroutines): array
    {
        $result = [];
        foreach ($coroutines as $id => $job) {
            if ($job->isCompleted()) {
                $this->config->logger->debug("filtered Coroutine $id, coz it completed");
                continue;
            }

            $result[$id] = $job;
        }

        return $result;
    }

    public function close(): void
    {
        $this->config->logger->info("closing stream ");
        $this->redis->setConsumerCanceled($this->config->consumer, true);
        $this->redis->close();
    }

    private function lag(): int
    {
        $infos = $this->redis->info();
        foreach ($infos as $info) {
            $group = $info['name'] ?? '';
            if ($group === $this->config->group) {
                return $info['lag'] ?? 0;
            }
        }

        return 0;
    }

    /**
     * @param array<string, Coroutine> $coroutines
     * @return void
     */
    private function waitAll(array $coroutines): void
    {
        $this->config->logger->info("waiting all unfinished coroutines");
        while (true) {
            $coroutines = $this->filter($coroutines);
            if (count($coroutines) === 0) {
                break;
            }
            suspend();
        }
        $this->redis->setConsumerCanceled($this->config->consumer, true);

        $this->config->logger->info("all coroutines finished");
    }
}
