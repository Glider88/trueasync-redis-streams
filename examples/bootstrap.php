<?php declare(strict_types=1);

use Glider88\AsyncRedisStreams\Backpressure\PiecewiseLinearScaling;
use Glider88\AsyncRedisStreams\Config;
use Glider88\AsyncRedisStreams\Logger\ConsoleLogger;
use Glider88\AsyncRedisStreams\Logger\NullLogger;
use Glider88\AsyncRedisStreams\Retry\MultiplyRetry;
use Glider88\AsyncRedisStreams\TimeInterval\Milli;
use Glider88\AsyncRedisStreams\TimeInterval\Sec;

require  __DIR__ . '/../vendor/autoload.php';

function mkConfig(): Config
{
    return new Config(
        redisUrl: 'redis://redis:6379',
        stream: 's',
        group: 'g',
        scaling: new PiecewiseLinearScaling([
            12 => 0,
            32 => 500,
        ]),
        healthcheckInterval: new Sec(100),
        maxStreamLength: 1000,
        maxDlqStreamLength: 1000,
        readRetrySetCount: 100,
        readAutoClaimCount: 100,
        blockRead: new Sec(1),
        deduplicationTtl: new Sec(3),
        autoClaimMinIdle: new Sec(2),
        timeoutJob: new Sec(1),
        retryInterval: new Milli(100),
        maxRetries: 3,
        retry: new MultiplyRetry(
            firstOffsetDelay: new Milli(0),
            baseDelay: new Sec(1),
        ),
        claimInterval: new Milli(100),
        logger: new NullLogger(),
//        logger: new ConsoleLogger(),
        minRedisWorkers: 1,
        maxRedisWorkers: 100,
        threadBootloader: static function () {
            require  '/usr/src/app/examples/bootstrap.php';
            require  '/usr/src/app/vendor/autoload.php';
        },
        consumer: 'c'
    );
}

