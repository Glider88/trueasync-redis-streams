<?php declare(strict_types=1);

namespace Glider88\AsyncRedisStreams\Redis;

use Glider88\AsyncRedisStreams\Config;
use Glider88\AsyncRedisStreams\Helpers\Arr;
use Glider88\AsyncRedisStreams\Helpers\Json;
use Glider88\AsyncRedisStreams\Helpers\Time;
use Glider88\AsyncRedisStreams\Logger\LoggerInterface;
use Glider88\AsyncRedisStreams\Stream;
use Glider88\AsyncRedisStreams\TimeInterval\TimeIntervalInterface;

readonly class Redis implements RedisInterface
{
    private string $consumer;

    public function __construct(
        private string                    $stream,
        private string                    $group,
        private ClientInterface           $redis,
        private ClientInterface           $redisBlocked,
        private LoggerInterface           $logger,
        private int                       $maxStreamLength,
        private int                       $maxDlqStreamLength,
        private int                       $readRetrySetCount,
        private int                       $readAutoClaimCount,
        private TimeIntervalInterface     $blockRead,
        private TimeIntervalInterface     $deduplicationTtl,
        private TimeIntervalInterface     $autoClaimMinIdle,
        ?string                           $consumer,
    ) {
        if (is_null($consumer)) {
            $this->consumer = 'c-'.gethostname().'-'.getmypid();
        } else {
            $this->consumer = $consumer;
        }
    }

    public static function fromConfig(Config $cfg): self
    {
        return new Redis(
            stream: $cfg->stream,
            group: $cfg->group,
            redis: PoolPredisWrapper::fromConfig($cfg),
            redisBlocked: PredisClientFactory::fromConfig($cfg)->make(),
            logger: $cfg->logger,
            maxStreamLength: $cfg->maxStreamLength,
            maxDlqStreamLength: $cfg->maxDlqStreamLength,
            readRetrySetCount: $cfg->readRetrySetCount,
            readAutoClaimCount: $cfg->readAutoClaimCount,
            blockRead: $cfg->blockRead,
            deduplicationTtl: $cfg->deduplicationTtl,
            autoClaimMinIdle: $cfg->autoClaimMinIdle,
            consumer: $cfg->consumer,
        );
    }

    public function createConsumerGroupIfNotExist(): void
    {
        $this->createConsumerGroup($this->stream);
        $this->createConsumerGroup($this->stream . ':dlq');
    }

    private function createConsumerGroup(string $stream): void
    {
        // auto id = $
        $args = ['XGROUP', 'CREATE', $stream, $this->group, '$', 'MKSTREAM'];

        try {
            $this->redis->execute($args);
            $this->logger->info("Consumer group: $this->group for stream: $stream created");
        } catch (\Exception $e) {
            if (str_contains($e->getMessage(), 'BUSYGROUP')) {
                $this->logger->info("Consumer group: $this->group for stream: $stream already exists");
            } else {
                throw $e;
            }
        }
    }

    /**  @return string redis stream message id */
    public function add(array $message): string
    {
        $args = ['XADD', $this->stream, 'MAXLEN', '~', $this->maxStreamLength, '*', ];

        foreach ($message as $k => $v) {
            $args[] = $k;
            $args[] = $v;
        }

        return $this->redis->execute($args);
    }

    /**  @return string redis stream message id */
    public function addToDlq(array $message): string
    {
        $args = ['XADD', $this->stream . ':dlq', 'MAXLEN', '~', $this->maxDlqStreamLength, '*', ];

        foreach ($message as $k => $v) {
            $args[] = $k;
            $args[] = $v;
        }

        return $this->redis->execute($args);
    }

    public function read(int $count): array
    {
        if ($count <= 0) {
            return [];
        }

        $args = [
            'XREADGROUP',
            'GROUP',
            $this->group,
            $this->consumer,
            'COUNT',
            $count,
            'BLOCK',
            $this->blockRead->milli(),
            'STREAMS',
            $this->stream,
            '>',
        ];

        $raw = $this->redisBlocked->execute($args);
        if ($raw === null) {
            return [];
        }

        $result = [];
        foreach ($raw as $row) {
            foreach (Arr::listPairsToArray($row, stub: []) as $stream => $rowValues) {
                foreach ($rowValues as $rowValue) {
                    foreach (Arr::listPairsToArray($rowValue, stub: []) as $id => $fields) {
                        $result[$stream][$id] = Arr::listPairsToArray($fields);
                    }
                }
            }
        }

        return $result;
    }

    /** @param string $id redis stream message */
    public function ack(string $id): void
    {
        $this->redis->execute(['XACK', $this->stream, $this->group, $id]);
    }

    /** @param string $id message id */
    public function passThroughGuard(string $id): bool
    {
        $key = $id . '-' . $this->group;
        $args = ['SET', $key, '1', 'EX', $this->deduplicationTtl->sec(), 'NX'];

        return $this->redis->execute($args) === ClientInterface::OK;
    }

    /** @param string $id message id */
    public function revokeGuard(string $id): void
    {
        $key = $id . '-' . $this->group;
        $this->redis->execute(['DEL', $key]);
    }

    public function addToRetry(int $startAt, array $message): void
    {
        $key = $this->stream . ':' . $this->group . ':retry';
        $messageStr = Json::encode($message);
        $this->redis->execute(['ZADD', $key, $startAt, $messageStr]);
    }

    public function retryRange(): array
    {
        $key = $this->stream . ':' . $this->group . ':retry';
        $args = ['ZRANGE', $key, 0, Time::nowMs(), 'BYSCORE', 'LIMIT', '0', $this->readRetrySetCount];
        $raw = $this->redis->execute($args);
        $serializeFn = static fn($row) => Json::decode($row);

        return array_map($serializeFn, $raw);
    }

    public function removeFromRetry(array $message): void
    {
        $key = $this->stream . ':' . $this->group . ':retry';
        $messageStr = Json::encode($message);
        $this->redis->execute(['ZREM', $key, $messageStr]);
    }

    /** @param string $cursorId current redis stream message id */
    public function autoClaim(string $cursorId): array
    {
        $args = [
            'XAUTOCLAIM',
            $this->stream,
            $this->group,
            $this->consumer,
            $this->autoClaimMinIdle->milli(),
            $cursorId,
            'COUNT',
            $this->readAutoClaimCount
        ];

        $raw = $this->redis->execute($args);

        $cursorId = $raw[0];
        $rows = $raw[1];

        $result = [];
        foreach ($rows as $row) {
            foreach (Arr::listPairsToArray($row, stub: []) as $messageId => $rowValues) {
                $result[$messageId] = Arr::listPairsToArray($rowValues);
            }
        }

        return [$cursorId, $result];
    }

    public function info(): array
    {
        $raw = $this->redis->execute(['XINFO', 'GROUPS', $this->stream]);

        return array_map(static fn($row) => Arr::listPairsToArray($row), $raw);
    }

    public function close(): void
    {
        $this->redis->close();
        $this->redisBlocked->close();
    }

    public function setConsumerCanceled(string $consumer, bool $isCanceled): void
    {
        $this->redis->execute(['SET', Stream::canceledKey($consumer), (int) $isCanceled]);
    }

    public function isConsumerCanceled(string $consumer): bool
    {
        $result = $this->redis->execute(['GET', Stream::canceledKey($consumer)]);
        if ($result === null) {
            return true;
        }

        return (bool) $result;
    }
}
