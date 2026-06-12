# TrueAsync redis streams realization

### Installation:
```shell
composer require glider88/async-redis-streams
```
Start docker:
```shell
bin/re # first time
```
```shell
bin/up # next times
```
Tests:
```shell
bin/unit
```

### Run example:

Producer:
```shell
bin/php examples/producer.php
```
Consumer:
```shell
bin/php examples/consumer.php
```

### Settings:
```php
$config = new Config(
    redisUrl: 'redis://redis:6379',
    stream: 's',                                          // stream name
    group: 'g',                                           // consumer group name
    scaling: new PiecewiseLinearScaling([                 // complex scaling number of workers, for empty stream (< 500) use 16 worker, next 32
        12 => 0,
        32 => 500,
    ]),
    healthcheckInterval: new Sec(100),                     // redis health check interval
    maxStreamLength: 1000,                                 // approximate stream size
    maxDlqStreamLength: 1000,                              // approximate stream size for dead letters
    readRetrySetCount: 100,                                // how many entities we get at time from redis sorted set
    readAutoClaimCount: 100,                               // how many autoclaim entities we get at time
    blockRead: new Sec(1),                                 // how long we wait first data from stream
    deduplicationTtl: new Sec(3),                          // for deduplication logic used `SET $streamMessageId . '-' . $this->group` with this ttl
    autoClaimMinIdle: new Sec(2),                          // after this time we get message from PEL by autoclaim
    timeoutJob: new Sec(1),                                // timeout for job, after which we cancel it by TimeoutCancellation exception
    retryInterval: new Milli(100),                         // how often we launch retry logic
    maxRetries: 3,                                         // after we send message to dead letters stream (with s:dql name)
    retry: new MultiplyRetry(                              // retry with incremental increase time: 0 1 2 3... seconds wait before retry
        firstOffsetDelay: new Milli(0),
        baseDelay: new Sec(1),
    ),
    claimInterval: new Milli(100),                         // how often we launch autoclaim logic
    logger: new NullLogger(),
    minRedisWorkers: 1,                                    // Async\Pool: How many redis connections to create in advance (pre-warming)
    maxRedisWorkers: 100,                                  // Async\Pool: Maximum redis connections
    threadBootloader: static function () {                 // Async\Thread bootloader
        require  '/usr/src/app/examples/bootstrap.php';
        require  '/usr/src/app/vendor/autoload.php';
    },
    consumer: 'c'                                          // consumer name
);

$stream = new Stream($config);
```
