<?php declare(strict_types=1);

namespace Glider88\AsyncRedisStreams;

use Closure;
use Glider88\AsyncRedisStreams\Backpressure\ConstantScaling;
use Glider88\AsyncRedisStreams\Backpressure\ScalingInterface;
use Glider88\AsyncRedisStreams\Helpers\Fun;
use Glider88\AsyncRedisStreams\Logger\LoggerInterface;
use Glider88\AsyncRedisStreams\Logger\NullLogger;
use Glider88\AsyncRedisStreams\Retry\ConstantRetry;
use Glider88\AsyncRedisStreams\Retry\RetryInterface;
use Glider88\AsyncRedisStreams\TimeInterval\Milli;
use Glider88\AsyncRedisStreams\TimeInterval\Sec;
use Glider88\AsyncRedisStreams\TimeInterval\TimeIntervalInterface;

readonly class Config
{
    public function __construct(
        public string                  $redisUrl,
        public string                  $stream,
        public string                  $group,
        public ScalingInterface        $scaling,
        public TimeIntervalInterface   $healthcheckInterval,
        public int                     $maxStreamLength,
        public int                     $maxDlqStreamLength,
        public int                     $readRetrySetCount,
        public int                     $readAutoClaimCount,
        public TimeIntervalInterface   $blockRead,
        public TimeIntervalInterface   $deduplicationTtl,
        public TimeIntervalInterface   $autoClaimMinIdle,
        public TimeIntervalInterface   $timeoutJob,
        public TimeIntervalInterface   $retryInterval,
        public int                     $maxRetries,
        public RetryInterface          $retry,
        public TimeIntervalInterface   $claimInterval,
        public LoggerInterface         $logger,
        public int                     $minRedisWorkers,
        public int                     $maxRedisWorkers,
        public Closure                 $threadBootloader,
        public string                  $consumer,
    ) {}

    public static function fromArray(array $options): self
    {
        return new Config(...array_merge(self::defaults(), $options));
    }

    public static function defaults(): array
    {
        return [
            'stream' => 'default',
            'group' => 'default',
            'consumer' => 'default',
            'scaling' => new ConstantScaling(100),
            'healthcheckInterval' => new Sec(15),
            'maxStreamLength' => 10_000,
            'maxDlqStreamLength' => 10_000,
            'readRetrySetCount' => 100,
            'readAutoClaimCount' => 100,
            'blockRead' => new Milli(100),
            'deduplicationTtl' => new Sec(10),
            'autoClaimMinIdle' => new Sec(2),
            'timeoutJob' => new Sec(1),
            'retryInterval' => new Milli(100),
            'maxRetries' => 3,
            'retry' => new ConstantRetry(new Sec(1)),
            'claimInterval' => new Milli(100),
            'logger' => new NullLogger(),
            'minRedisWorkers' => 1,
            'maxRedisWorkers' => 10,
            'threadBootloader' => Fun::void(),
        ];
    }
}
